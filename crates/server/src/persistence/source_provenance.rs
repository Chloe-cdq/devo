//! Shared, streaming source headers for recovery and passive admission.
//! Payloads are skipped; complete conversation validation stays with replay.

use std::collections::HashSet;
use std::fmt;
use std::io::Read;

use anyhow::Result;
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
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdentityHeader {
    id: Option<String>,
    #[serde(alias = "session_id")]
    session_id: Option<String>,
}

#[derive(Deserialize)]
struct LegacySessionHeader {
    session: IdentityHeader,
}

#[derive(Deserialize)]
struct LegacyIdentityHeader {
    session_id: Option<String>,
    turn: Option<IdentityHeader>,
    item: Option<IdentityHeader>,
    record: Option<IdentityHeader>,
}

#[derive(Deserialize)]
struct InternalHeader {
    #[serde(rename = "type")]
    kind: Option<String>,
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
                        "session" | "turn" | "item" | "record" => {
                            let identity: IdentityHeader = map.next_value()?;
                            header.sources.extend(if key == "session" {
                                identity.id
                            } else {
                                identity.session_id
                            });
                        }
                        "entry" => {
                            let entry: InternalHeader = map.next_value()?;
                            header.record_kind = entry.kind;
                        }
                        "SessionMeta" => {
                            let legacy: LegacySessionHeader = map.next_value()?;
                            header.sources.extend(legacy.session.id);
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
                        | "SessionRollback"
                        | "SessionSettings" => {
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
