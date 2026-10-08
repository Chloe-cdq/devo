//! Shared current source admission for automatic scans and explicit rebuilds.
use super::MemoryRuntime;
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

impl MemoryRuntime {
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
        .await?
        .unwrap_or(Err(SourceExclusion::SourceUnavailable));
        match exclusion {
            Ok(identity) if identity == source_id => {}
            Ok(_) => {
                self.note_source_exclusion(SourceExclusion::InvalidHistory);
                return Ok(SourceAdmission::Excluded);
            }
            Err(reason) => {
                self.note_source_exclusion(reason);
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
        let source = tokio::task::spawn_blocking(move || read_source(&read_path))
            .await?
            .ok()
            .flatten();
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
