//! Shared current source admission for automatic scans and explicit rebuilds.
use super::MemoryRuntime;
use super::jobs::{JobClaim, JobFailure};
use super::scan::ScanContext;
use super::source::{ExtractableSource, MAX_SOURCE_BYTES, read_source};
use devo_protocol::native::rpc_memory::MemorySourceExclusionReason as SourceExclusion;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;

pub(super) enum SourceAdmission {
    Ready {
        path: PathBuf,
        source: ExtractableSource,
    },
    Deferred,
    Excluded,
}

pub(super) enum SourceReread {
    Ready(ExtractableSource),
    Ineligible,
    Failed,
}

impl MemoryRuntime {
    /// Keep I/O failure distinct from ineligible history after a job is claimed.
    pub(super) async fn reread_claimed_source(
        self: &Arc<Self>,
        path: PathBuf,
        claim: &JobClaim,
    ) -> anyhow::Result<SourceReread> {
        match tokio::task::spawn_blocking(move || read_source(&path)).await? {
            Ok(Some(source)) => Ok(SourceReread::Ready(source)),
            Ok(None) => Ok(SourceReread::Ineligible),
            Err(_) => {
                // Filesystem diagnostics may contain source paths or private text.
                self.note_storage_failure();
                let memory = Arc::clone(self);
                let claim = claim.clone();
                tokio::task::spawn_blocking(move || {
                    memory.fail_job(&claim, JobFailure::SourceRead, chrono::Utc::now())
                })
                .await??;
                Ok(SourceReread::Failed)
            }
        }
    }

    pub(super) async fn read_scan_source(
        self: &Arc<Self>,
        context: &ScanContext,
        index: crate::db::SessionIndexRecord,
    ) -> anyhow::Result<SourceAdmission> {
        let session_id = index.metadata.session_id;
        let source_id = session_id.to_string();
        let exclusion = if index.metadata.ephemeral {
            Some(SourceExclusion::Ephemeral)
        } else if index.metadata.parent_session_id.is_some() || index.metadata.agent_path.is_some()
        {
            Some(SourceExclusion::NonRoot)
        } else if index.metadata.fork_from_id.is_some() {
            Some(SourceExclusion::ForkHistory)
        } else if index.rollout_path.is_none() {
            Some(SourceExclusion::NotPersisted)
        } else {
            None
        };
        if let Some(reason) = exclusion {
            self.note_source_exclusion(reason);
            return Ok(SourceAdmission::Excluded);
        }
        let path = index.rollout_path.expect("persistent source path");
        let header_path = path.clone();
        let exclusion = tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(header_path)?;
            Ok::<_, std::io::Error>(crate::persistence::read_source_eligibility(BufReader::new(
                file.take(MAX_SOURCE_BYTES + 1),
            )))
        })
        .await?;
        let exclusion = match exclusion {
            Ok(exclusion) => exclusion,
            Err(_) => {
                self.note_storage_failure();
                self.note_source_exclusion(SourceExclusion::SourceUnavailable);
                return Ok(SourceAdmission::Deferred);
            }
        };
        match exclusion {
            Ok(identity) if identity == source_id => {}
            Ok(_) => {
                self.note_source_exclusion(SourceExclusion::InvalidHistory);
                return Ok(SourceAdmission::Excluded);
            }
            Err(reason) => {
                self.note_source_exclusion(reason);
                if reason == SourceExclusion::SourceUnavailable {
                    self.note_storage_failure();
                    return Ok(SourceAdmission::Deferred);
                }
                return Ok(SourceAdmission::Excluded);
            }
        }
        if self.scan_source_has_intent(&source_id).await {
            self.note_source_exclusion(SourceExclusion::SourceFenced);
            return Ok(SourceAdmission::Deferred);
        }
        if context.activity.is_active(session_id).await {
            self.note_source_exclusion(SourceExclusion::Active);
            return Ok(SourceAdmission::Deferred);
        }
        let read_path = path.clone();
        let source = match tokio::task::spawn_blocking(move || read_source(&read_path)).await? {
            Ok(source) => source,
            Err(_) => {
                self.note_storage_failure();
                self.note_source_exclusion(SourceExclusion::SourceUnavailable);
                return Ok(SourceAdmission::Deferred);
            }
        };
        let Some(source) = source else {
            self.note_source_exclusion(SourceExclusion::InvalidHistory);
            return Ok(SourceAdmission::Excluded);
        };
        if source.session_id.as_str() != source_id.as_str() {
            self.note_source_exclusion(SourceExclusion::InvalidHistory);
            return Ok(SourceAdmission::Excluded);
        }
        Ok(SourceAdmission::Ready { path, source })
    }
}
