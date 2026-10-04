//! Shared, streaming source headers for recovery and passive admission.
//! Payloads are skipped; complete conversation validation stays with replay.

use std::collections::HashSet;
use std::fmt;
use std::io::Read;

use anyhow::Result;
use devo_protocol::native::rpc_memory::MemorySourceExclusionReason;
use serde::Deserialize;
use serde::de::{IgnoredAny, MapAccess, Visitor};

#[derive(Default)]
struct SourceHeader {
    version: Option<u32>,
    kind: Option<String>,
    record_kind: Option<String>,
    sources: Vec<String>,
    legacy_rows: usize,
    other_fields: bool,
    session: Option<SessionHeader>,
    settings: Option<InternalHeader>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentityHeader {
    #[serde(alias = "session_id")]
    session_id: Option<String>,
}

#[derive(Deserialize)]
struct LegacySessionHeader {
    session: SessionHeader,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionHeader {
    id: Option<String>,
    ephemeral: Option<serde_json::Value>,
    source: Option<serde_json::Value>,
    parent: Option<IgnoredAny>,
    #[serde(alias = "parent_session_id")]
    parent_session_id: Option<IgnoredAny>,
    #[serde(alias = "fork_from_id")]
    fork_from_id: Option<IgnoredAny>,
    #[serde(alias = "agent_path")]
    agent_path: Option<IgnoredAny>,
    #[serde(alias = "agent_nickname")]
    agent_nickname: Option<IgnoredAny>,
    #[serde(alias = "agent_role")]
    agent_role: Option<IgnoredAny>,
}

impl SessionHeader {
    fn valid(&self) -> bool {
        self.ephemeral
            .as_ref()
            .is_none_or(serde_json::Value::is_boolean)
            && self
                .source
                .as_ref()
                .is_none_or(serde_json::Value::is_string)
    }

    fn exclusion(&self) -> Option<MemorySourceExclusionReason> {
        use MemorySourceExclusionReason as Reason;
        let source = self
            .source
            .as_ref()
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        if self.ephemeral.as_ref().and_then(serde_json::Value::as_bool) == Some(true) {
            Some(Reason::Ephemeral)
        } else if self.parent.is_some()
            || self.parent_session_id.is_some()
            || self.agent_path.is_some()
            || self.agent_nickname.is_some()
            || self.agent_role.is_some()
            || source.contains("subagent")
        {
            Some(Reason::NonRoot)
        } else if ["autom", "heartbeat", "cron"]
            .iter()
            .any(|marker| source.contains(marker))
        {
            Some(Reason::Automation)
        } else if self.fork_from_id.is_some() {
            Some(Reason::ForkHistory)
        } else {
            None
        }
    }
}

#[derive(Deserialize)]
struct LegacySettingsHeader {
    session_id: String,
    field: serde_json::Value,
    value: serde_json::Value,
}

#[derive(Deserialize)]
struct LegacyIdentityHeader {
    session_id: Option<String>,
    turn: Option<IdentityHeader>,
    item: Option<IdentityHeader>,
    record: Option<IdentityHeader>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InternalHeader {
    #[serde(rename = "type")]
    kind: Option<String>,
    schema_version: Option<serde_json::Value>,
    field: Option<serde_json::Value>,
    value: Option<serde_json::Value>,
}

impl<'de> Deserialize<'de> for SourceHeader {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct HeaderVisitor;
        impl<'de> Visitor<'de> for HeaderVisitor {
            type Value = SourceHeader;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a rollout source header")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut header = SourceHeader::default();
                let mut seen = HashSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !seen.insert(key.clone()) {
                        return Err(serde::de::Error::custom("duplicate source header field"));
                    }
                    match key.as_str() {
                        "v" => header.version = Some(map.next_value()?),
                        "kind" => header.kind = Some(map.next_value()?),
                        "sessionId" => header.sources.push(map.next_value()?),
                        "session" => {
                            let session: SessionHeader = map.next_value()?;
                            header.sources.extend(session.id.iter().cloned());
                            header.session = Some(session);
                        }
                        "turn" | "item" | "record" => {
                            let identity: IdentityHeader = map.next_value()?;
                            header.sources.extend(identity.session_id);
                        }
                        "entry" => {
                            let entry: InternalHeader = map.next_value()?;
                            header.record_kind = entry.kind.clone();
                            header.settings = Some(entry);
                        }
                        "SessionMeta" => {
                            let legacy: LegacySessionHeader = map.next_value()?;
                            header.sources.extend(legacy.session.id.iter().cloned());
                            header.session = Some(legacy.session);
                            header.legacy_rows += 1;
                        }
                        "Turn"
                        | "Item"
                        | "SessionTitleUpdated"
                        | "SessionContextUpdated"
                        | "CompactionSnapshot"
                        | "MessageEditRecorded"
                        | "TurnSuperseded"
                        | "TurnWorkspaceCheckpointRecorded"
                        | "TurnWorkspaceChangeRecorded"
                        | "TurnWorkspaceRestoreStarted"
                        | "TurnWorkspaceRestoreCompleted"
                        | "SessionRollback" => {
                            let legacy: LegacyIdentityHeader = map.next_value()?;
                            header.sources.extend(legacy.session_id);
                            for identity in [legacy.turn, legacy.item, legacy.record]
                                .into_iter()
                                .flatten()
                            {
                                header.sources.extend(identity.session_id);
                            }
                            header.legacy_rows += 1;
                        }
                        "SessionSettings" => {
                            let settings: LegacySettingsHeader = map.next_value()?;
                            header.sources.push(settings.session_id);
                            header.settings = Some(InternalHeader {
                                kind: Some("sessionSettings".into()),
                                schema_version: Some(serde_json::json!(1)),
                                field: Some(settings.field),
                                value: Some(settings.value),
                            });
                            header.legacy_rows += 1;
                        }
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                            header.other_fields = true;
                        }
                    }
                }
                Ok(header)
            }
        }
        deserializer.deserialize_map(HeaderVisitor)
    }
}

impl SourceHeader {
    fn supported(&self) -> bool {
        if self
            .session
            .as_ref()
            .is_some_and(|session| !session.valid())
        {
            return false;
        }
        match (self.version, self.kind.as_deref()) {
            (None, None) => self.legacy_rows == 1 && !self.other_fields,
            (Some(2), Some(kind)) if self.legacy_rows == 0 => match kind {
                "internal" => matches!(
                    self.record_kind.as_deref(),
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
                "sessionMeta"
                | "turn"
                | "item"
                | "sessionTitleUpdated"
                | "compactionSnapshot"
                | "sessionRollback"
                | "workspaceCheckpoint"
                | "workspaceChange"
                | "workspaceRestoreStarted"
                | "workspaceRestoreCompleted" => true,
                _ => false,
            },
            (Some(_), _) | (None, Some(_)) => false,
        }
    }
}

/// Inspect source identity and durable facts while skipping conversation
/// payloads. This runs before the passive reader allocates transcript text.
pub(crate) fn read_source_eligibility(
    reader: impl Read,
) -> std::result::Result<String, MemorySourceExclusionReason> {
    use MemorySourceExclusionReason as Reason;
    let mut reasons = std::collections::BTreeSet::new();
    let mut source = None;
    let mut has_session = false;
    for row in serde_json::Deserializer::from_reader(reader).into_iter::<SourceHeader>() {
        let header = row.map_err(|_| Reason::InvalidHistory)?;
        if !header.supported() {
            return Err(Reason::InvalidHistory);
        }
        for identity in &header.sources {
            if source.as_ref().is_some_and(|source| source != identity) {
                return Err(Reason::InvalidHistory);
            }
            source = Some(identity.clone());
        }
        if header.record_kind.as_deref() == Some("externalContextUsed") {
            reasons.insert(Reason::ExternalContextUsed);
        }
        if let Some(session) = &header.session {
            if session.id.is_none() {
                return Err(Reason::InvalidHistory);
            }
            has_session = true;
            if header.version == Some(2)
                && session
                    .source
                    .as_ref()
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|source| !matches!(source, "interactive" | "automation"))
            {
                return Err(Reason::InvalidHistory);
            }
            if let Some(reason) = session.exclusion() {
                reasons.insert(reason);
            }
        }
        if let Some(settings) = header.settings
            && settings.kind.as_deref() == Some("sessionSettings")
        {
            if settings
                .schema_version
                .as_ref()
                .and_then(serde_json::Value::as_u64)
                != Some(1)
            {
                return Err(Reason::InvalidHistory);
            }
            let field = settings
                .field
                .and_then(|field| {
                    serde_json::from_value::<devo_core::SessionSettingsField>(field).ok()
                })
                .ok_or(Reason::InvalidHistory)?;
            if field == devo_core::SessionSettingsField::SessionSource {
                match settings
                    .value
                    .and_then(|value| serde_json::from_value(value).ok())
                {
                    Some(devo_protocol::native::session::SessionSource::Interactive) => {}
                    Some(devo_protocol::native::session::SessionSource::Automation) => {
                        reasons.insert(Reason::Automation);
                    }
                    None => return Err(Reason::InvalidHistory),
                }
            }
        }
    }
    if !has_session {
        return Err(Reason::InvalidHistory);
    }
    if let Some(reason) = reasons.into_iter().next() {
        return Err(reason);
    }
    source.ok_or(Reason::InvalidHistory)
}

/// Read the committed fact independently of any store's cached write state.
/// Damaged history cannot certify an unmarked source for fork inheritance.
pub(super) fn read_external_context_used(reader: impl Read) -> Result<bool> {
    let mut marked = false;
    for row in serde_json::Deserializer::from_reader(reader).into_iter::<SourceHeader>() {
        let header = row.map_err(|_| anyhow::anyhow!("rollout provenance unavailable"))?;
        anyhow::ensure!(header.supported(), "rollout provenance unavailable");
        marked |= header.record_kind.as_deref() == Some("externalContextUsed");
    }
    Ok(marked)
}

/// Return excluded identities, conservatively quarantining an identifiable
/// damaged history. Unidentified damage never reopens inferred memory.
pub(crate) fn read_source_exclusions(reader: impl Read) -> Result<HashSet<String>> {
    let mut known = HashSet::new();
    let mut excluded = HashSet::new();
    for row in serde_json::Deserializer::from_reader(reader).into_iter::<SourceHeader>() {
        let Ok(header) = row else {
            anyhow::ensure!(!known.is_empty(), "rollout provenance unavailable");
            excluded.extend(known);
            return Ok(excluded);
        };
        known.extend(header.sources.iter().cloned());
        if !header.supported() {
            anyhow::ensure!(!known.is_empty(), "rollout provenance unavailable");
            excluded.extend(known);
            return Ok(excluded);
        }
        if header.record_kind.as_deref() == Some("externalContextUsed") {
            anyhow::ensure!(!known.is_empty(), "rollout provenance unavailable");
            excluded.extend(known.iter().cloned());
        }
    }
    anyhow::ensure!(!known.is_empty(), "rollout provenance unavailable");
    Ok(excluded)
}

#[cfg(test)]
mod tests {
    use super::read_source_exclusions;
    use pretty_assertions::assert_eq;
    use std::collections::HashSet;

    /// Trace: L1-REQ-MEM-001 Acceptance, L2-DES-MEM-001 Rev 4 DD-7/DD-13.
    /// Verifies: null versions and duplicate headers cannot turn damaged provenance into an eligible source.
    #[test]
    fn malformed_headers_quarantine_known_source() {
        for damaged in [
            r#"{"v":null,"kind":null,"Turn":{"turn":{"session_id":"source"}}}"#,
            r#"{"v":99,"v":2,"kind":"turn","turn":{"sessionId":"source"}}"#,
            r#"{"v":2,"kind":"unknown","kind":"turn","turn":{"sessionId":"source"}}"#,
        ] {
            let journal =
                format!(r#"{{"v":2,"kind":"turn","turn":{{"sessionId":"source"}}}} {damaged}"#);
            assert_eq!(
                read_source_exclusions(journal.as_bytes()).unwrap(),
                HashSet::from(["source".to_string()])
            );
        }
    }
}
