use std::collections::HashSet;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::Result;
use chrono::Utc;
use devo_core::{InternalRecordV2, RolloutLineV2, SessionId};
use devo_protocol::native::ids::SessionId as NativeSessionId;

use super::{RolloutStore, WritePathState};

impl RolloutStore {
    /// Recover canonical source facts in the background. Capture a committed
    /// prefix under the append lock, then release it before scanning history.
    pub(crate) fn external_context_sources(&self) -> Result<HashSet<SessionId>> {
        let mut sources = HashSet::new();
        for path in self.rollout_paths()? {
            let (file, length) = self.with_locked_file(&path, || {
                let file = std::fs::File::open(&path)?;
                let length = file.metadata()?.len();
                Ok((file, length))
            })?;
            let excluded = super::read_source_exclusions(BufReader::new(file.take(length)))?;
            for source in excluded {
                sources.insert(source.parse()?);
            }
        }
        Ok(sources)
    }

    /// Read the monotonic fact under the append lock, including any pending
    /// retry. A fork cannot publish history while source durability is unknown.
    pub(crate) fn external_context_used_at(&self, rollout_path: &Path) -> Result<bool> {
        let (file, length) = self.with_locked_write_state(rollout_path, |_state| {
            let file = std::fs::File::open(rollout_path)?;
            let length = file.metadata()?.len();
            Ok((file, length))
        })?;
        super::source_provenance::read_external_context_used(BufReader::new(file.take(length)))
    }

    /// Intent belongs to ordinary session persistence even when optional
    /// memory cannot initialize. A failed append is retried by the next write.
    pub(crate) fn mark_external_context_used_at(
        &self,
        rollout_path: &Path,
        session_id: SessionId,
    ) -> Result<()> {
        self.pending_external_context
            .lock()
            .expect("rollout provenance state poisoned")
            .insert(rollout_path.to_path_buf(), session_id);
        self.with_locked_write_state(rollout_path, |state| {
            self.retry_external_context_marker(rollout_path, state)
        })
    }

    pub(super) fn retry_external_context_marker(
        &self,
        rollout_path: &Path,
        state: &mut WritePathState,
    ) -> Result<()> {
        let source = self
            .pending_external_context
            .lock()
            .expect("rollout provenance state poisoned")
            .get(rollout_path)
            .copied();
        let Some(source) = source else {
            return Ok(());
        };
        if !state.external_context_used && rollout_path.exists() {
            let file = std::fs::File::open(rollout_path)?;
            let length = file.metadata()?.len();
            state.external_context_used = super::source_provenance::read_external_context_used(
                BufReader::new(file.take(length)),
            )?;
        }
        if !state.external_context_used {
            let line = RolloutLineV2::Internal {
                v: 2,
                timestamp: Utc::now(),
                session_id: NativeSessionId::from_legacy_uuid(uuid::Uuid::from(source)),
                turn_id: None,
                seq: state.next_line_index,
                entry: InternalRecordV2::ExternalContextUsed,
            };
            self.write_v2_lines(rollout_path, state, std::slice::from_ref(&line))?;
            state.projector.observe_v2_line(&line);
            state.external_context_used = true;
        }
        self.pending_external_context
            .lock()
            .expect("rollout provenance state poisoned")
            .remove(rollout_path);
        Ok(())
    }
}
