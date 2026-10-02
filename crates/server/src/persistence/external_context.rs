use std::collections::HashSet;
use std::io::{BufReader, Read};
use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use devo_core::{InternalRecordV2, RolloutLineV2, SessionId};
use devo_protocol::native::ids::SessionId as NativeSessionId;
use serde::Deserialize;

use super::{RolloutStore, WritePathState};

// Read only identity and provenance, streaming past message/tool payloads.
// Full history validation remains the passive source reader's responsibility.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProvenanceRow {
    v: Option<u32>,
    kind: Option<String>,
    session_id: Option<String>,
    session: Option<ProvenanceSession>,
    entry: Option<ProvenanceEntry>,
}

#[derive(Deserialize)]
struct ProvenanceSession {
    id: String,
}

#[derive(Deserialize)]
struct ProvenanceEntry {
    #[serde(rename = "type")]
    kind: String,
}

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
            let reader = BufReader::new(file.take(length));
            let rows = serde_json::Deserializer::from_reader(reader).into_iter::<ProvenanceRow>();
            let mut known_sources = HashSet::new();
            for row in rows {
                let Ok(row) = row else {
                    // A damaged/unfinished row makes this history uncertain,
                    // without preventing recovery of unrelated sessions.
                    anyhow::ensure!(!known_sources.is_empty(), "rollout provenance unavailable");
                    sources.extend(known_sources);
                    break;
                };
                let identity = row
                    .session_id
                    .as_deref()
                    .or_else(|| row.session.as_ref().map(|session| session.id.as_str()));
                if let Some(identity) = identity {
                    known_sources.insert(identity.parse::<SessionId>()?);
                }
                let supported = match (row.v, row.kind.as_deref()) {
                    (None, None) => true, // Frozen legacy format has neither v nor kind.
                    (Some(2), Some("internal")) => matches!(
                        row.entry.as_ref().map(|entry| entry.kind.as_str()),
                        Some(
                            "execution"
                                | "entry"
                                | "sessionContext"
                                | "messageEdit"
                                | "turnSuperseded"
                                | "goalState"
                                | "usageRecord"
                                | "externalContextUsed"
                                | "sessionSettings"
                                | "turnApprovalCheckpoint"
                        )
                    ),
                    (
                        Some(2),
                        Some(
                            "sessionMeta"
                            | "turn"
                            | "item"
                            | "sessionTitleUpdated"
                            | "compactionSnapshot"
                            | "sessionRollback"
                            | "workspaceCheckpoint"
                            | "workspaceChange"
                            | "workspaceRestoreStarted"
                            | "workspaceRestoreCompleted",
                        ),
                    ) => true,
                    (Some(_), _) | (None, Some(_)) => false,
                };
                if !supported {
                    anyhow::ensure!(!known_sources.is_empty(), "rollout provenance unavailable");
                    sources.extend(known_sources);
                    break;
                }
                if row.v == Some(2)
                    && row.kind.as_deref() == Some("internal")
                    && row
                        .entry
                        .as_ref()
                        .is_some_and(|entry| entry.kind == "externalContextUsed")
                {
                    let identity = row
                        .session_id
                        .context("rollout source identity unavailable")?;
                    sources.insert(identity.parse()?);
                }
            }
        }
        Ok(sources)
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
