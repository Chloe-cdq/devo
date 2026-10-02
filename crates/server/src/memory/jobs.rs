use super::source::ExtractableSource;
use super::{MemoryError, MemoryRuntime};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use devo_protocol::native::session::MemorySetting;
use rusqlite::{OptionalExtension, TransactionBehavior};

pub(super) const MAX_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct JobClaim {
    pub(super) id: String,
    pub(super) owner: String,
    pub(super) attempt: u32,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum JobFailure {
    TransientProvider,
    PermanentProvider,
    ProviderUnavailable,
    InvalidOutput,
    Credentials,
    Storage,
}

impl JobFailure {
    pub(super) fn class(self) -> &'static str {
        match self {
            Self::TransientProvider => "transient_provider_error",
            Self::PermanentProvider => "permanent_provider_error",
            Self::ProviderUnavailable => "provider_unavailable",
            Self::InvalidOutput => "invalid_structured_output",
            Self::Credentials => "credentials_unavailable",
            Self::Storage => "storage_error",
        }
    }
}

impl MemoryRuntime {
    pub(super) fn claim_source(
        &self,
        source: &ExtractableSource,
        now: DateTime<Utc>,
    ) -> Result<Option<JobClaim>, MemoryError> {
        let idle = now.signed_duration_since(source.observed_at);
        let minimum_idle = Duration::try_hours(
            self.config
                .min_source_idle_hours
                .try_into()
                .unwrap_or(i64::MAX),
        )
        .unwrap_or(Duration::MAX);
        let window = Duration::try_days(
            self.config
                .source_window_days
                .try_into()
                .unwrap_or(i64::MAX),
        )
        .unwrap_or(Duration::MAX);
        if self
            .config
            .resolve_contribution(source.session_contribution)
            != MemorySetting::On
            || source.messages.is_empty()
            || idle < minimum_idle
            || idle > window
        {
            return Ok(None);
        }
        if self.source_has_intent(source.session_id.as_str()) {
            return Ok(None);
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM memory_deleted_sources WHERE source_session_id = ?1)
                OR EXISTS(SELECT 1 FROM memory_excluded_sources WHERE source_session_id = ?1)
                OR EXISTS(SELECT 1 FROM memory_job_receipts
                    WHERE source_session_id = ?1 AND source_watermark = ?2)",
            rusqlite::params![source.session_id.as_str(), source.watermark],
            |row| row.get::<_, bool>(0),
        )? {
            return Ok(None);
        }
        let timestamp = now.to_rfc3339_opts(SecondsFormat::Millis, /*use_z*/ true);
        let lease_until = (now + Duration::minutes(2))
            .to_rfc3339_opts(SecondsFormat::Millis, /*use_z*/ true);
        let owner = uuid::Uuid::now_v7().simple().to_string();
        let job_id = uuid::Uuid::now_v7().simple().to_string();
        let job_key = format!("{}:{}", source.session_id, source.watermark);
        transaction.execute(
            "INSERT INTO memory_jobs (
                job_id, job_kind, job_key, source_session_id, source_watermark,
                state, created_at, updated_at
             ) VALUES (?1, 'source_scan', ?2, ?3, ?4, 'pending', ?5, ?5)
             ON CONFLICT(source_session_id, source_watermark) DO NOTHING",
            rusqlite::params![
                job_id,
                job_key,
                source.session_id.as_str(),
                source.watermark,
                timestamp
            ],
        )?;
        // A crashed final attempt must remain visible instead of being reclaimed forever.
        transaction.execute(
            "UPDATE memory_jobs SET state = 'error', error_class = 'transient_provider_error',
                lease_owner = NULL, lease_until = NULL, updated_at = ?1
             WHERE source_session_id = ?2 AND source_watermark = ?3
                AND state = 'running' AND lease_until <= ?1 AND attempt_count >= ?4",
            rusqlite::params![
                timestamp,
                source.session_id.as_str(),
                source.watermark,
                MAX_ATTEMPTS
            ],
        )?;
        let claim = transaction.query_row(
            "UPDATE memory_jobs SET state = 'running', attempt_count = attempt_count + 1,
                lease_owner = ?1, lease_until = ?2, claimed_at = ?3, updated_at = ?3, retry_at = NULL
             WHERE source_session_id = ?4 AND source_watermark = ?5
                AND attempt_count < ?6 AND (
                    state = 'pending'
                    OR (state = 'retrying' AND retry_at <= ?3)
                    OR (state = 'running' AND lease_until <= ?3)
                )
             RETURNING job_id, attempt_count",
            rusqlite::params![owner, lease_until, timestamp, source.session_id.as_str(), source.watermark, MAX_ATTEMPTS],
            |row| Ok(JobClaim { id: row.get(0)?, owner: owner.clone(), attempt: row.get(1)? }),
        ).optional()?;
        transaction.commit()?;
        Ok(claim)
    }

    pub(super) fn fail_job(
        &self,
        claim: &JobClaim,
        failure: JobFailure,
        now: DateTime<Utc>,
    ) -> Result<(), MemoryError> {
        let retry =
            matches!(failure, JobFailure::TransientProvider) && claim.attempt < MAX_ATTEMPTS;
        let retry_at = retry.then(|| {
            (now + Duration::seconds(30 * (1_i64 << (claim.attempt - 1))))
                .to_rfc3339_opts(SecondsFormat::Millis, /*use_z*/ true)
        });
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        connection.execute(
            "UPDATE memory_jobs SET state = ?1, retry_at = ?2, error_class = ?3,
                lease_owner = NULL, lease_until = NULL, updated_at = ?4
             WHERE job_id = ?5 AND lease_owner = ?6 AND state = 'running' AND lease_until > ?4",
            rusqlite::params![
                if retry { "retrying" } else { "error" },
                retry_at,
                failure.class(),
                now.to_rfc3339_opts(SecondsFormat::Millis, /*use_z*/ true),
                claim.id,
                claim.owner,
            ],
        )?;
        Ok(())
    }

    pub(super) fn release_job(&self, claim: &JobClaim) -> Result<(), MemoryError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        connection.execute(
            "UPDATE memory_jobs SET state = 'pending', attempt_count = attempt_count - 1,
                lease_owner = NULL, lease_until = NULL
             WHERE job_id = ?1 AND lease_owner = ?2 AND state = 'running'",
            rusqlite::params![claim.id, claim.owner],
        )?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "jobs_tests.rs"]
mod tests;
