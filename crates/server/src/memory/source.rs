//! Bounded, fail-closed rollout input for passive memory extraction.

use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read, Seek};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use devo_core::durable_execution::ExecutionRecord;
use devo_core::{
    InternalRecordV2, ParsedRolloutLine, ResponseItem, RolloutLine, RolloutLineV2,
    SessionSettingsField, TurnItem, V2InverseProjector, parse_rollout_line,
};
use devo_protocol::ContentBlock;
use devo_protocol::native::ids::{ItemId, SessionId, TurnId};
use devo_protocol::native::item::{Item, ItemEnvelope, ItemState, ToolSource};
use devo_protocol::native::session::{MemorySetting, SessionSource, SessionStatus};
use serde::Serialize;
use sha2::{Digest, Sha256};

pub(super) const MAX_SOURCE_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct ExtractableSource {
    pub(crate) session_id: SessionId,
    pub(crate) workspace_root: PathBuf,
    pub(crate) session_contribution: MemorySetting,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) watermark: String,
    /// Prior full-journal fingerprint, for upgrading existing scan receipts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) legacy_watermark: Option<String>,
    pub(crate) messages: Vec<SourceMessage>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct SourceMessage {
    pub(crate) turn_id: TurnId,
    pub(crate) item_id: ItemId,
    pub(crate) observed_at: DateTime<Utc>,
    pub(crate) role: String,
    pub(crate) text: String,
}

/// Returns no source when history completeness or provenance cannot be proved.
/// Parse errors are deliberately discarded: their text can contain source data.
pub(crate) fn read_source(path: &Path) -> anyhow::Result<Option<ExtractableSource>> {
    let mut file = std::fs::File::open(path)?;
    let metadata = file.metadata()?;
    #[cfg(test)]
    super::source_read_test_support::run(
        path,
        super::source_read_test_support::ReadPoint::AfterMetadata,
    );
    if !metadata.is_file() || metadata.len() > MAX_SOURCE_BYTES {
        return Ok(None);
    }
    // Every read, including pre-attempt and pre-commit rereads, skips payloads
    // until the complete committed source prefix passes admission.
    if crate::persistence::read_source_eligibility(BufReader::new(
        file.by_ref().take(metadata.len()),
    ))
    .is_err()
    {
        return Ok(None);
    }
    file.rewind()?;
    #[cfg(test)]
    super::source_read_test_support::run(
        path,
        super::source_read_test_support::ReadPoint::BeforeTranscript,
    );
    // A bounded prefix must not hide facts appended after length capture.
    if file.metadata()?.len() != metadata.len() {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    file.by_ref().take(metadata.len()).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() || file.metadata()?.len() != metadata.len() {
        return Ok(None);
    }
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Ok(None);
    };
    let inverse = V2InverseProjector::new();
    let mut session: Option<devo_core::SessionRecord> = None;
    let mut turns = HashMap::<devo_protocol::TurnId, devo_core::TurnRecord>::new();
    let mut items = HashMap::<devo_protocol::ItemId, devo_core::ItemRecord>::new();
    let mut native_items = HashMap::<String, ItemEnvelope>::new();
    let mut observed_at = DateTime::<Utc>::MIN_UTC;
    let mut snapshot_contribution = MemorySetting::Inherit;
    let mut field_contribution = None;
    let mut native_session_pending = false;
    let mut watermark = Sha256::new();

    for raw in text.split_inclusive('\n') {
        if raw.trim().is_empty() {
            watermark.update(raw.as_bytes());
            continue;
        }
        let Ok(parsed) = parse_rollout_line(raw) else {
            // Unlike interactive resume, extraction never tolerates a crash tail.
            return Ok(None);
        };
        // A background call may be accounted to this retained source itself.
        // Its verified accounting record neither changes extractable history nor
        // makes the session active. All semantic journal bytes remain in the hash.
        if let ParsedRolloutLine::V2(line) = &parsed
            && let RolloutLineV2::Internal {
                session_id,
                turn_id: None,
                entry: InternalRecordV2::UsageRecord { record },
                ..
            } = line.as_ref()
            && record.session_id == *session_id
            && record.turn_id.is_none()
            && record.purpose == devo_protocol::native::usage::UsagePurpose::MemoryExtraction
        {
            if inverse.project_line(line).is_err() {
                return Ok(None);
            }
            continue;
        }
        watermark.update(raw.as_bytes());
        let lines = match parsed {
            ParsedRolloutLine::Legacy(line) => vec![*line],
            ParsedRolloutLine::V2(line) => {
                let timestamp = match line.as_ref() {
                    RolloutLineV2::SessionMeta { timestamp, .. }
                    | RolloutLineV2::Turn { timestamp, .. }
                    | RolloutLineV2::Item { timestamp, .. }
                    | RolloutLineV2::Internal { timestamp, .. }
                    | RolloutLineV2::SessionTitleUpdated { timestamp, .. }
                    | RolloutLineV2::CompactionSnapshot { timestamp, .. }
                    | RolloutLineV2::SessionRollback { timestamp, .. }
                    | RolloutLineV2::WorkspaceCheckpoint { timestamp, .. }
                    | RolloutLineV2::WorkspaceChange { timestamp, .. }
                    | RolloutLineV2::WorkspaceRestoreStarted { timestamp, .. }
                    | RolloutLineV2::WorkspaceRestoreCompleted { timestamp, .. } => *timestamp,
                };
                observed_at = observed_at.max(timestamp);
                match line.as_ref() {
                    RolloutLineV2::SessionMeta { session, .. } => {
                        if session.ephemeral || !session.source.is_interactive() {
                            return Ok(None);
                        }
                        snapshot_contribution = session.settings.memory_contribution;
                        native_session_pending = session.status == SessionStatus::Active
                            || session.active_turn_id.is_some()
                            || session.queued_count > 0
                            || !session.flags.is_empty();
                    }
                    RolloutLineV2::Item { item, .. } => {
                        // The inverse projector drops MCP provenance, so inspect it first.
                        if let Item::ToolCall {
                            tool_name,
                            source,
                            server_name,
                            input,
                            ..
                        } = &item.item
                            && (*source != ToolSource::Builtin
                                || server_name.is_some()
                                || external_tool(tool_name, input.as_ref()))
                        {
                            return Ok(None);
                        }
                        if matches!(item.item, Item::HostedToolCall { .. }) {
                            return Ok(None);
                        }
                        observed_at = observed_at.max(item.created_at).max(item.updated_at);
                        if let Some(previous) = native_items.get(item.id.as_str()) {
                            if previous.turn_id != item.turn_id || previous.seq != item.seq {
                                return Ok(None);
                            }
                            if previous.revision == item.revision && previous != item {
                                return Ok(None);
                            }
                            if previous.revision >= item.revision {
                                continue;
                            }
                        }
                        native_items.insert(item.id.as_str().to_owned(), item.clone());
                    }
                    RolloutLineV2::Internal {
                        entry: InternalRecordV2::Execution { record },
                        ..
                    } => {
                        let responses = match record {
                            ExecutionRecord::ModelCompleted { items, .. }
                            | ExecutionRecord::PromptCheckpoint { items, .. }
                            | ExecutionRecord::IntentBatch { calls: items }
                            | ExecutionRecord::Outcomes { results: items } => items.as_slice(),
                            ExecutionRecord::OutputArtifacts { .. }
                            | ExecutionRecord::Recovery { .. } => &[],
                        };
                        for response in responses {
                            let external = match response {
                                ResponseItem::ToolCall { name, input, .. } => {
                                    external_tool(name, Some(input))
                                }
                                ResponseItem::Message(message) => {
                                    message.content.iter().any(|block| match block {
                                        ContentBlock::ToolUse { name, input, .. } => {
                                            external_tool(name, Some(input))
                                        }
                                        ContentBlock::HostedToolUse { .. } => true,
                                        ContentBlock::Text { .. }
                                        | ContentBlock::Reasoning { .. }
                                        | ContentBlock::ProviderReasoning { .. }
                                        | ContentBlock::ToolResult { .. } => false,
                                    })
                                }
                                ResponseItem::Reason { .. }
                                | ResponseItem::ToolCallOutput { .. } => false,
                            };
                            if external {
                                return Ok(None);
                            }
                        }
                    }
                    RolloutLineV2::Internal {
                        entry: InternalRecordV2::ExternalContextUsed,
                        ..
                    } => return Ok(None),
                    RolloutLineV2::Internal {
                        entry: InternalRecordV2::SessionSettings { schema_version, .. },
                        ..
                    } if *schema_version != 1 => return Ok(None),
                    RolloutLineV2::Turn { .. }
                    | RolloutLineV2::Internal { .. }
                    | RolloutLineV2::SessionTitleUpdated { .. }
                    | RolloutLineV2::CompactionSnapshot { .. }
                    | RolloutLineV2::SessionRollback { .. }
                    | RolloutLineV2::WorkspaceCheckpoint { .. }
                    | RolloutLineV2::WorkspaceChange { .. }
                    | RolloutLineV2::WorkspaceRestoreStarted { .. }
                    | RolloutLineV2::WorkspaceRestoreCompleted { .. } => {}
                }
                let Ok(lines) = inverse.project_line(&line) else {
                    return Ok(None);
                };
                lines
            }
        };
        for line in lines {
            let timestamp = match &line {
                RolloutLine::SessionMeta(line) => line.timestamp,
                RolloutLine::Turn(line) => line.timestamp,
                RolloutLine::Item(line) => line.timestamp,
                RolloutLine::SessionTitleUpdated(line) => line.timestamp,
                RolloutLine::SessionContextUpdated(line) => line.timestamp,
                RolloutLine::CompactionSnapshot(line) => line.timestamp,
                RolloutLine::MessageEditRecorded(line) => line.timestamp,
                RolloutLine::TurnSuperseded(line) => line.timestamp,
                RolloutLine::TurnWorkspaceCheckpointRecorded(line) => line.timestamp,
                RolloutLine::TurnWorkspaceChangeRecorded(line) => line.timestamp,
                RolloutLine::TurnWorkspaceRestoreStarted(line) => line.timestamp,
                RolloutLine::TurnWorkspaceRestoreCompleted(line) => line.timestamp,
                RolloutLine::SessionRollback(line) => line.timestamp,
                RolloutLine::SessionSettings(line) => line.timestamp,
            };
            observed_at = observed_at.max(timestamp);
            match line {
                RolloutLine::SessionMeta(line) => {
                    let record = line.session;
                    let source = record.source.to_ascii_lowercase();
                    if record.schema_version == 0
                        || record.schema_version > 2
                        || record.parent_session_id.is_some()
                        || record.agent_path.is_some()
                        || record.agent_nickname.is_some()
                        || record.agent_role.is_some()
                        || record.fork_from_id.is_some()
                        || ["autom", "heartbeat", "cron", "subagent"]
                            .iter()
                            .any(|marker| source.contains(marker))
                        || session
                            .as_ref()
                            .is_some_and(|previous| previous.id != record.id)
                    {
                        return Ok(None);
                    }
                    observed_at = observed_at
                        .max(record.created_at)
                        .max(record.updated_at)
                        .max(record.last_activity_at.unwrap_or(record.created_at));
                    session = Some(record);
                }
                RolloutLine::Turn(line) => {
                    if !session
                        .as_ref()
                        .is_some_and(|session| session.id == line.turn.session_id)
                        || line.turn.schema_version == 0
                        || line.turn.schema_version > 4
                    {
                        return Ok(None);
                    }
                    observed_at = observed_at
                        .max(line.turn.started_at)
                        .max(line.turn.completed_at.unwrap_or(line.turn.started_at));
                    turns.insert(line.turn.id, line.turn);
                }
                RolloutLine::Item(line) => {
                    if !session
                        .as_ref()
                        .is_some_and(|session| session.id == line.item.session_id)
                        || line.item.schema_version != 1
                    {
                        return Ok(None);
                    }
                    observed_at = observed_at
                        .max(line.item.timestamp)
                        .max(line.item.started_at.unwrap_or(line.item.timestamp));
                    for payload in line.item.input_items.iter().chain(&line.item.output_items) {
                        let external = match payload {
                            TurnItem::ToolCall(call) => {
                                external_tool(&call.tool_name, Some(&call.input))
                            }
                            TurnItem::ToolResult(result) => result
                                .tool_name
                                .as_ref()
                                .is_some_and(|name| external_tool_name(name)),
                            TurnItem::CommandExecution(command) => {
                                external_tool(&command.tool_name, Some(&command.input))
                            }
                            TurnItem::WebSearch(_) => true,
                            TurnItem::UserMessage(_)
                            | TurnItem::SteerInput(_)
                            | TurnItem::HookPrompt(_)
                            | TurnItem::AgentMessage(_)
                            | TurnItem::Plan(_)
                            | TurnItem::Reasoning(_)
                            | TurnItem::ToolProgress(_)
                            | TurnItem::ApprovalRequest(_)
                            | TurnItem::ApprovalDecision(_)
                            | TurnItem::ImageGeneration(_)
                            | TurnItem::ContextCompaction(_)
                            | TurnItem::TurnSummary(_) => false,
                        };
                        if external {
                            return Ok(None);
                        }
                    }
                    items.insert(line.item.id, line.item);
                }
                RolloutLine::SessionSettings(line) => {
                    if !session
                        .as_ref()
                        .is_some_and(|session| session.id == line.session_id)
                    {
                        return Ok(None);
                    }
                    if line.field == SessionSettingsField::SessionSource {
                        let Ok(SessionSource::Interactive) =
                            serde_json::from_value::<SessionSource>(line.value)
                        else {
                            return Ok(None);
                        };
                    } else if line.field == SessionSettingsField::MemoryContribution {
                        let Ok(setting) = serde_json::from_value::<MemorySetting>(line.value)
                        else {
                            return Ok(None);
                        };
                        field_contribution = Some(setting);
                    }
                }
                RolloutLine::SessionRollback(line) => {
                    if !session
                        .as_ref()
                        .is_some_and(|session| session.id == line.session_id)
                        || line.schema_version != 1
                    {
                        return Ok(None);
                    }
                    let retained_turns: HashSet<_> = line.retained_turn_ids.into_iter().collect();
                    let retained_items: HashSet<_> = line.retained_item_ids.into_iter().collect();
                    let retained_native: HashSet<_> =
                        retained_items.iter().map(ToString::to_string).collect();
                    turns.retain(|id, _| retained_turns.contains(id));
                    items.retain(|id, item| {
                        retained_items.contains(id) && retained_turns.contains(&item.turn_id)
                    });
                    native_items.retain(|id, _| retained_native.contains(id));
                }
                RolloutLine::MessageEditRecorded(line) => {
                    if !session
                        .as_ref()
                        .is_some_and(|session| session.id == line.record.session_id)
                    {
                        return Ok(None);
                    }
                    match line.record.edit_state {
                        devo_core::EditState::Rejected => {}
                        devo_core::EditState::Accepted
                        | devo_core::EditState::RestorePending
                        | devo_core::EditState::ReplacementStarted
                        | devo_core::EditState::QueuedUpdated => {
                            items.remove(&line.record.target_message_id);
                            native_items.remove(&line.record.target_message_id.to_string());
                        }
                    }
                }
                RolloutLine::TurnSuperseded(line) => {
                    if !session
                        .as_ref()
                        .is_some_and(|session| session.id == line.record.session_id)
                    {
                        return Ok(None);
                    }
                    let superseded = line.record.superseded_turn_id;
                    turns.remove(&superseded);
                    items.retain(|_, item| item.turn_id != superseded);
                    let native_turn_id = superseded.to_string();
                    native_items.retain(|_, item| item.turn_id.as_str() != native_turn_id);
                }
                RolloutLine::SessionTitleUpdated(_)
                | RolloutLine::SessionContextUpdated(_)
                | RolloutLine::CompactionSnapshot(_)
                | RolloutLine::TurnWorkspaceCheckpointRecorded(_)
                | RolloutLine::TurnWorkspaceChangeRecorded(_)
                | RolloutLine::TurnWorkspaceRestoreStarted(_)
                | RolloutLine::TurnWorkspaceRestoreCompleted(_) => {}
            }
        }
    }
    let Some(session) = session else {
        return Ok(None);
    };
    if native_session_pending
        || !session.cwd.is_absolute()
        || turns.is_empty()
        || turns.values().any(|turn| {
            turn.completed_at.is_none()
                || match turn.status {
                    devo_protocol::TurnStatus::Pending
                    | devo_protocol::TurnStatus::Running
                    | devo_protocol::TurnStatus::WaitingApproval => true,
                    devo_protocol::TurnStatus::Interrupted | devo_protocol::TurnStatus::Failed => {
                        false
                    }
                    devo_protocol::TurnStatus::Completed => false,
                }
        })
        || native_items.values().any(|item| {
            matches!(item.state, ItemState::Running | ItemState::Waiting)
                && !devo_protocol::TurnId::try_from(item.turn_id.as_str())
                    .ok()
                    .and_then(|turn_id| turns.get(&turn_id))
                    .is_some_and(|turn| {
                        matches!(
                            turn.status,
                            devo_protocol::TurnStatus::Failed
                                | devo_protocol::TurnStatus::Interrupted
                        )
                    })
        })
    {
        return Ok(None);
    }
    let mut items: Vec<_> = items.into_values().collect();
    items.sort_by_key(|item| item.seq);
    let mut messages = Vec::new();
    for item in items {
        if !turns.contains_key(&item.turn_id) {
            return Ok(None);
        }
        if !turns
            .get(&item.turn_id)
            .is_some_and(|turn| turn.status == devo_protocol::TurnStatus::Completed)
        {
            continue;
        }
        if native_items
            .get(&item.id.to_string())
            .is_some_and(|item| item.state != ItemState::Completed)
        {
            continue;
        }
        for (position, payload) in item
            .input_items
            .iter()
            .chain(&item.output_items)
            .enumerate()
        {
            let (role, text) = match payload {
                TurnItem::UserMessage(text) | TurnItem::SteerInput(text) => ("user", &text.text),
                TurnItem::AgentMessage(text) if position >= item.input_items.len() => {
                    ("assistant", &text.text)
                }
                TurnItem::AgentMessage(_)
                | TurnItem::HookPrompt(_)
                | TurnItem::Plan(_)
                | TurnItem::Reasoning(_)
                | TurnItem::ToolCall(_)
                | TurnItem::ToolProgress(_)
                | TurnItem::ToolResult(_)
                | TurnItem::CommandExecution(_)
                | TurnItem::ApprovalRequest(_)
                | TurnItem::ApprovalDecision(_)
                | TurnItem::WebSearch(_)
                | TurnItem::ImageGeneration(_)
                | TurnItem::ContextCompaction(_)
                | TurnItem::TurnSummary(_) => continue,
            };
            if !text.trim().is_empty() {
                messages.push(SourceMessage {
                    turn_id: TurnId::from_string(item.turn_id.to_string()),
                    item_id: ItemId::from_string(item.id.to_string()),
                    observed_at: item.timestamp,
                    role: role.into(),
                    text: text.clone(),
                });
            }
        }
    }
    let source_text = messages
        .iter()
        .map(|message| message.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    if messages.is_empty() || super::entries::contains_secret(&source_text) {
        return Ok(None);
    }
    // Preserve the raw-byte identity of every semantic journal line.
    let watermark = format!("{:x}", watermark.finalize());
    let legacy_watermark = format!("{:x}", Sha256::digest(&bytes));
    let legacy_watermark = (legacy_watermark != watermark).then_some(legacy_watermark);
    #[cfg(test)]
    super::source_read_test_support::run(
        path,
        super::source_read_test_support::ReadPoint::Complete,
    );
    Ok(Some(ExtractableSource {
        session_id: SessionId::from_string(session.id.to_string()),
        workspace_root: session.cwd,
        session_contribution: field_contribution.unwrap_or(snapshot_contribution),
        observed_at,
        watermark,
        legacy_watermark,
        messages,
    }))
}

#[path = "source_external.rs"]
mod external;
use external::{external_tool, external_tool_name};

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "source_usage_tests.rs"]
mod usage_tests;
