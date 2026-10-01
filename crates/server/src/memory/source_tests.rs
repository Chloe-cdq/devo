use std::path::Path;

use devo_core::{LegacyProjector, ParsedRolloutLine, parse_rollout_line};
use pretty_assertions::assert_eq;
use serde_json::{Value, json};
use tempfile::TempDir;

use super::*;

const SESSION: &str = "00000000-0000-0000-0000-0000000000b1";
const TURN: &str = "00000000-0000-0000-0000-0000000000b2";
const ITEM: &str = "00000000-0000-0000-0000-0000000000b3";

fn legacy(workspace: &Path) -> Vec<Value> {
    let fixture = include_str!("../../../core/tests/fixtures/rollout_v1/basic_session.jsonl");
    let mut lines: Vec<Value> = fixture
        .lines()
        .take(3)
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    lines[0]["SessionMeta"]["session"]["cwd"] = json!(workspace);
    lines
}

fn write_lines(dir: &TempDir, lines: &[Value]) -> PathBuf {
    let path = dir.path().join("source.jsonl");
    let text = lines
        .iter()
        .map(|line| serde_json::to_string(line).unwrap())
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(&path, text).unwrap();
    path
}

fn v2(lines: &[Value]) -> Vec<Value> {
    let mut projector = LegacyProjector::new();
    lines
        .iter()
        .flat_map(|value| {
            let ParsedRolloutLine::Legacy(line) = parse_rollout_line(&value.to_string()).unwrap()
            else {
                panic!("legacy fixture")
            };
            projector
                .project_line(&line)
                .unwrap()
                .into_iter()
                .map(|line| serde_json::to_value(line).unwrap())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: reads only explicit user and assistant text with source identity.
#[test]
fn reads_only_explicit_user_and_assistant_text_with_source_identity() {
    let dir = TempDir::new().unwrap();
    let path = write_lines(&dir, &legacy(dir.path()));
    let source = read_source(&path).unwrap().expect("eligible source");
    assert_eq!(
        source,
        ExtractableSource {
            session_id: SessionId::from_string(SESSION.into()),
            workspace_root: dir.path().into(),
            session_contribution: MemorySetting::Inherit,
            observed_at: "2026-07-01T12:00:11Z".parse().unwrap(),
            watermark: source.watermark.clone(),
            messages: vec![
                SourceMessage {
                    turn_id: TurnId::from_string(TURN.into()),
                    item_id: ItemId::from_string(ITEM.into()),
                    observed_at: "2026-07-01T12:00:11Z".parse().unwrap(),
                    role: "user".into(),
                    text: "Fix the flaky test".into()
                },
                SourceMessage {
                    turn_id: TurnId::from_string(TURN.into()),
                    item_id: ItemId::from_string(ITEM.into()),
                    observed_at: "2026-07-01T12:00:11Z".parse().unwrap(),
                    role: "assistant".into(),
                    text: "On it.".into()
                },
            ],
        }
    );
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: field contribution survives later whole session metadata.
#[test]
fn field_contribution_survives_later_whole_session_metadata() {
    let dir = TempDir::new().unwrap();
    let mut lines = v2(&legacy(dir.path()));
    lines.push(json!({"v":2,"kind":"internal","timestamp":"2026-07-01T13:00:00Z","sessionId":SESSION,"turnId":null,"seq":0,"entry":{"type":"sessionSettings","schemaVersion":1,"field":"memoryContribution","value":"off","epoch":1}}));
    lines.push(lines[0].clone());
    let path = write_lines(&dir, &lines);
    let source = read_source(&path).unwrap().unwrap();
    assert_eq!(
        (source.session_contribution, source.observed_at),
        (MemorySetting::Off, "2026-07-01T13:00:00Z".parse().unwrap())
    );
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: skips parent subagent automation and fork sessions.
#[test]
fn skips_parent_subagent_automation_and_fork_sessions() {
    let dir = TempDir::new().unwrap();
    for (field, value) in [
        (
            "parent_session_id",
            json!("00000000-0000-0000-0000-000000000010"),
        ),
        ("agent_path", json!("/root/child")),
        ("agent_role", json!("worker")),
        ("agent_nickname", json!("child")),
        ("source", json!("automation")),
        (
            "fork_from_id",
            json!("00000000-0000-0000-0000-000000000010"),
        ),
    ] {
        let mut lines = legacy(dir.path());
        lines[0]["SessionMeta"]["session"][field] = value;
        assert_eq!(
            read_source(&write_lines(&dir, &lines)).unwrap(),
            None,
            "{field}"
        );
    }
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: rejects incomplete turn and corrupt or truncated tail.
#[test]
fn rejects_incomplete_turn_and_corrupt_or_truncated_tail() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    lines[1]["Turn"]["turn"]["status"] = json!("Running");
    lines[1]["Turn"]["turn"]["completed_at"] = Value::Null;
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
    for suffix in ["{\"v\":2", "{}", "{\"v\":999}"] {
        let path = write_lines(&dir, &legacy(dir.path()));
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(suffix.as_bytes())
            .unwrap();
        assert_eq!(read_source(&path).unwrap(), None, "{suffix}");
    }
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: tool calls taint the entire session even before rollback.
#[test]
fn tool_calls_taint_the_entire_session_even_before_rollback() {
    let dir = TempDir::new().unwrap();
    for name in [
        "web.run",
        "web_search",
        "mcp__docs__search",
        "tools_search",
        "functions.tool_search",
    ] {
        let mut lines = legacy(dir.path());
        let mut call = lines[2].clone();
        call["Item"]["item"]["id"] = json!("00000000-0000-0000-0000-000000000099");
        call["Item"]["item"]["seq"] = json!(2);
        call["Item"]["item"]["input_items"] = json!([]);
        call["Item"]["item"]["output_items"] =
            json!([{"ToolCall":{"tool_call_id":"remote","tool_name":name,"input":{}}}]);
        lines.push(call);
        lines.push(json!({"SessionRollback":{"timestamp":"2026-07-01T12:00:20Z","session_id":SESSION,"retained_turn_ids":[TURN],"retained_item_ids":[ITEM],"latest_turn_id":TURN,"schema_version":1}}));
        assert_eq!(
            read_source(&write_lines(&dir, &lines)).unwrap(),
            None,
            "{name}"
        );
    }
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: detects native mcp tool source even without mcp name.
#[test]
fn detects_native_mcp_tool_source_even_without_mcp_name() {
    let dir = TempDir::new().unwrap();
    let mut lines = v2(&legacy(dir.path()));
    let mut item = lines
        .iter()
        .find(|line| line["kind"] == "item")
        .unwrap()
        .clone();
    item["item"]["id"] = json!("00000000-0000-0000-0000-000000000099");
    item["item"]["seq"] = json!(99);
    item["item"]["item"] =
        json!({"type":"toolCall","callId":"remote","toolName":"search","source":"mcp","input":{}});
    lines.push(item);
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: an ordinary failed turn does not taint a later clean completed turn.
#[test]
fn clean_failed_turn_followed_by_success_is_eligible() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    lines[1]["Turn"]["turn"]["status"] = json!("Failed");
    let mut later_turn = lines[1].clone();
    later_turn["Turn"]["turn"]["id"] = json!("00000000-0000-0000-0000-0000000000c2");
    later_turn["Turn"]["turn"]["status"] = json!("Completed");
    lines.push(later_turn);
    let mut later_item = lines[2].clone();
    later_item["Item"]["item"]["id"] = json!("00000000-0000-0000-0000-0000000000c3");
    later_item["Item"]["item"]["turn_id"] = json!("00000000-0000-0000-0000-0000000000c2");
    lines.push(later_item);
    assert!(read_source(&write_lines(&dir, &lines)).unwrap().is_some());
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-6.
/// Verifies: a pending native item from a failed turn cannot taint a later clean turn.
#[test]
fn failed_native_item_does_not_taint_later_completed_turn() {
    let dir = TempDir::new().unwrap();
    let mut legacy_lines = legacy(dir.path());
    legacy_lines[1]["Turn"]["turn"]["status"] = json!("Failed");
    let mut later_turn = legacy_lines[1].clone();
    later_turn["Turn"]["turn"]["id"] = json!("00000000-0000-0000-0000-0000000000c2");
    later_turn["Turn"]["turn"]["status"] = json!("Completed");
    legacy_lines.push(later_turn);
    let mut later_item = legacy_lines[2].clone();
    later_item["Item"]["item"]["id"] = json!("00000000-0000-0000-0000-0000000000c3");
    later_item["Item"]["item"]["turn_id"] = json!("00000000-0000-0000-0000-0000000000c2");
    legacy_lines.push(later_item);
    let mut lines = v2(&legacy_lines);
    let failed_item = lines
        .iter_mut()
        .find(|line| line["kind"] == "item" && line["item"]["turnId"] == TURN)
        .unwrap();
    failed_item["item"]["state"] = json!("running");

    assert!(read_source(&write_lines(&dir, &lines)).unwrap().is_some());
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: a completed revision of a failed turn is eligible when no external context was used.
#[test]
fn clean_failed_turn_superseded_by_completion_is_eligible() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    lines[1]["Turn"]["turn"]["status"] = json!("Failed");
    let mut replacement = lines[1].clone();
    replacement["Turn"]["turn"]["status"] = json!("Completed");
    lines.push(replacement);
    assert!(read_source(&write_lines(&dir, &lines)).unwrap().is_some());
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: a durable session fact excludes an otherwise clean source.
#[test]
fn external_context_marker_excludes_clean_source() {
    let dir = TempDir::new().unwrap();
    let mut lines = v2(&legacy(dir.path()));
    let marker = json!({"v":2,"kind":"internal","timestamp":"2026-07-01T12:00:20Z","sessionId":SESSION,"turnId":null,"seq":99,"entry":{"type":"externalContextUsed"}});
    assert!(matches!(
        parse_rollout_line(&marker.to_string()),
        Ok(ParsedRolloutLine::V2(_))
    ));
    lines.push(marker);
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: hidden context tools and attachments never enter messages.
#[test]
fn hidden_context_tools_and_attachments_never_enter_messages() {
    let dir = TempDir::new().unwrap();
    let mut lines = v2(&legacy(dir.path()));
    let mut item = lines
        .iter()
        .find(|line| line["kind"] == "item")
        .unwrap()
        .clone();
    item["item"]["id"] = json!("00000000-0000-0000-0000-000000000099");
    item["item"]["seq"] = json!(99);
    item["item"]["item"] = json!({"type":"userMessage","entry":"turnStart","content":[{"type":"text","text":"plain text"},{"type":"mention","uri":"credential://private"},{"type":"image","uri":"attachment"}]});
    lines.push(item);
    let source = read_source(&write_lines(&dir, &lines)).unwrap().unwrap();
    assert_eq!(
        source
            .messages
            .iter()
            .map(|message| message.text.as_str())
            .collect::<Vec<_>>(),
        vec!["Fix the flaky test", "On it.", "plain text"]
    );
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: credentials are excluded from extraction.
#[test]
fn credentials_are_excluded_from_extraction() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    lines[2]["Item"]["item"]["input_items"][0]["UserMessage"]["text"] =
        json!("My token is sk-123456789012345678901234");
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: watermark changes when content is rewritten without length change.
#[test]
fn watermark_changes_when_content_is_rewritten_without_length_change() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    let path = write_lines(&dir, &lines);
    let first = read_source(&path).unwrap().unwrap().watermark;
    lines[2]["Item"]["item"]["output_items"][0]["AgentMessage"]["text"] = json!("Fixed!");
    let second = read_source(&write_lines(&dir, &lines))
        .unwrap()
        .unwrap()
        .watermark;
    assert_ne!(first, second);
    assert_eq!(second, read_source(&path).unwrap().unwrap().watermark);
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: bounds rollout bytes before parsing.
#[test]
fn bounds_rollout_bytes_before_parsing() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    lines[2]["Item"]["item"]["input_items"][0]["UserMessage"]["text"] =
        json!("x".repeat(1024 * 1024));
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: folds latest turn and item revisions instead of extracting stale text.
#[test]
fn folds_latest_turn_and_item_revisions_instead_of_extracting_stale_text() {
    let dir = TempDir::new().unwrap();
    let mut lines = v2(&legacy(dir.path()));
    let mut item = lines
        .iter()
        .find(|line| line["item"]["item"]["type"] == "assistantMessage")
        .unwrap()
        .clone();
    item["item"]["revision"] = json!(2);
    item["item"]["item"]["text"] = json!("Fixed.");
    item["item"]["updatedAt"] = json!("2026-07-01T14:00:00Z");
    lines.push(item);
    let source = read_source(&write_lines(&dir, &lines)).unwrap().unwrap();
    assert_eq!(
        (
            source.observed_at,
            source
                .messages
                .iter()
                .map(|message| message.text.as_str())
                .collect::<Vec<_>>()
        ),
        (
            "2026-07-01T14:00:00Z".parse().unwrap(),
            vec!["Fix the flaky test", "Fixed."]
        )
    );
    let mut turn = lines
        .iter()
        .find(|line| line["kind"] == "turn")
        .unwrap()
        .clone();
    turn["turn"]["status"] = json!("inProgress");
    turn["turn"]["completedAt"] = Value::Null;
    lines.push(turn);
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: rollback retains only live items and fails closed for unknown turns.
#[test]
fn rollback_retains_only_live_items_and_fails_closed_for_unknown_turns() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    lines.push(json!({"SessionRollback":{"timestamp":"2026-07-01T12:00:20Z","session_id":SESSION,"retained_turn_ids":[TURN],"retained_item_ids":[],"latest_turn_id":TURN,"schema_version":1}}));
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
    lines.pop();
    lines[2]["Item"]["item"]["turn_id"] = json!("00000000-0000-0000-0000-000000000010");
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: context activity defers idle gating without becoming source text.
#[test]
fn context_activity_defers_idle_gating_without_becoming_source_text() {
    let dir = TempDir::new().unwrap();
    let mut lines = v2(&legacy(dir.path()));
    lines.push(json!({"v":2,"kind":"internal","timestamp":"2026-07-01T15:00:00Z","sessionId":SESSION,"turnId":TURN,"seq":9,"entry":{"type":"entry","entry":{"type":"hookPrompt","text":"private system instruction"}}}));
    let source = read_source(&write_lines(&dir, &lines)).unwrap().unwrap();
    assert_eq!(
        (
            source.observed_at,
            source
                .messages
                .iter()
                .map(|message| message.text.as_str())
                .collect::<Vec<_>>()
        ),
        (
            "2026-07-01T15:00:00Z".parse().unwrap(),
            vec!["Fix the flaky test", "On it."]
        )
    );
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: tool wrappers cannot hide external calls in their input.
#[test]
fn tool_wrappers_cannot_hide_external_calls_in_their_input() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    lines[2]["Item"]["item"]["output_items"].as_array_mut().unwrap().push(json!({"ToolCall":{"tool_call_id":"remote","tool_name":"functions.exec","input":{"code":"await tools.web__run({search_query: [{q: 'private'}]})"}}}));
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: malformed or future contribution setting cannot reenable extraction.
#[test]
fn malformed_or_future_contribution_setting_cannot_reenable_extraction() {
    let dir = TempDir::new().unwrap();
    for (schema_version, value) in [(1, json!("unexpected")), (2, json!("on"))] {
        let mut lines = v2(&legacy(dir.path()));
        lines.push(json!({"v":2,"kind":"internal","timestamp":"2026-07-01T13:00:00Z","sessionId":SESSION,"turnId":null,"seq":0,"entry":{"type":"sessionSettings","schemaVersion":schema_version,"field":"memoryContribution","value":value,"epoch":1}}));
        assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
    }
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: native ephemeral or pending item sessions are ineligible.
#[test]
fn native_ephemeral_or_pending_item_sessions_are_ineligible() {
    let dir = TempDir::new().unwrap();
    let mut lines = v2(&legacy(dir.path()));
    lines[0]["session"]["ephemeral"] = json!(true);
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
    lines[0]["session"]["ephemeral"] = json!(false);
    let item = lines
        .iter_mut()
        .find(|line| line["kind"] == "item")
        .unwrap();
    item["item"]["state"] = json!("running");
    assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: message evidence keeps item time when later settings change.
#[test]
fn message_evidence_keeps_item_time_when_later_settings_change() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    lines.push(json!({"SessionSettings":{"timestamp":"2026-07-01T16:00:00Z","session_id":SESSION,"field":"memoryContribution","value":"on","epoch":1}}));
    let source = read_source(&write_lines(&dir, &lines)).unwrap().unwrap();
    assert_eq!(
        (
            source.observed_at,
            source
                .messages
                .iter()
                .map(|message| message.observed_at)
                .collect::<Vec<_>>()
        ),
        (
            "2026-07-01T16:00:00Z".parse().unwrap(),
            vec![
                "2026-07-01T12:00:11Z".parse().unwrap(),
                "2026-07-01T12:00:11Z".parse().unwrap()
            ]
        )
    );
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: durable execution calls taint source without public tool items.
#[test]
fn durable_execution_calls_taint_source_without_public_tool_items() {
    let dir = TempDir::new().unwrap();
    let base = legacy(dir.path());
    assert!(read_source(&write_lines(&dir, &base)).unwrap().is_some());
    let remote = devo_core::ResponseItem::ToolCall {
        id: "remote".into(),
        name: "mcp__docs__lookup".into(),
        input: json!({}),
    };
    let hosted = devo_core::ResponseItem::from(devo_protocol::ContentBlock::HostedToolUse {
        id: "hosted".into(),
        name: "web_search".into(),
        input: json!({}),
        output: None,
        status: None,
    });
    for record in [
        devo_core::durable_execution::ExecutionRecord::IntentBatch {
            calls: vec![remote.clone()],
        },
        devo_core::durable_execution::ExecutionRecord::PromptCheckpoint {
            items: vec![remote],
            counters: None,
        },
        devo_core::durable_execution::ExecutionRecord::ModelCompleted {
            items: vec![hosted],
            stop_reason: None,
        },
    ] {
        let mut lines = base.clone();
        lines.push(json!({"v":2,"kind":"internal","timestamp":"2026-07-01T16:00:00Z","sessionId":SESSION,"turnId":TURN,"seq":0,"entry":{"type":"execution","record":record}}));
        assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
    }
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: decided native approvals do not hide safe conversation messages.
#[test]
fn decided_native_approvals_do_not_hide_safe_conversation_messages() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    let fixture = include_str!("../../../core/tests/fixtures/rollout_v1/basic_session.jsonl");
    lines.extend(
        fixture
            .lines()
            .skip(5)
            .take(2)
            .map(|line| serde_json::from_str::<Value>(line).unwrap()),
    );
    let source = read_source(&write_lines(&dir, &v2(&lines)))
        .unwrap()
        .expect("terminal approval source");
    assert_eq!(
        source
            .messages
            .iter()
            .map(|message| message.text.as_str())
            .collect::<Vec<_>>(),
        vec!["Fix the flaky test", "On it."]
    );
}

/// Trace: L2-DES-MEM-001 Rev 4.
/// Verifies: local tool output is excluded without becoming external provenance.
#[test]
fn local_tool_output_is_excluded_without_becoming_external_provenance() {
    let dir = TempDir::new().unwrap();
    let mut lines = legacy(dir.path());
    let fixture = include_str!("../../../core/tests/fixtures/rollout_v1/basic_session.jsonl");
    let mut tool: Value = serde_json::from_str(fixture.lines().nth(3).unwrap()).unwrap();
    tool["Item"]["item"]["output_items"][1]["ToolResult"]["output"] =
        json!({"content":"mcp__docs__search and Bearer abcdefghijklmnopqrstuvwxyz"});
    lines.push(tool);
    let source = read_source(&write_lines(&dir, &lines))
        .unwrap()
        .expect("local-tool source");
    assert_eq!(
        source
            .messages
            .iter()
            .map(|message| message.text.as_str())
            .collect::<Vec<_>>(),
        vec!["Fix the flaky test", "On it."]
    );
}

/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: even short explicit credential assignments never become extractor input.
#[test]
fn short_credential_assignments_are_excluded_from_extraction() {
    let dir = TempDir::new().unwrap();
    for text in [
        "API key: \" \"",
        "password=;",
        "_API_KEY=ab",
        "_password=ab",
        "API key: ab",
        "API key = abcdefghijklmnop",
        "Credentials: API key: ab",
        "option = password=ab",
        "API\nkey=ab",
        "API\u{2003}key=ab",
        "API key:\nab",
        "API key:\u{2003}ab",
        "password=1",
        "password=1234567",
        "password : \"1234\"",
        "\"password\": \"1234567\"",
        "token=abc",
        "secret = x",
        "api-key: x",
        "apikey=x",
        "access_token=abc",
        "client_secret=x",
        "db_password=1",
        "authToken=abc",
    ] {
        let mut lines = legacy(dir.path());
        lines[2]["Item"]["item"]["input_items"][0]["UserMessage"]["text"] = json!(text);
        assert_eq!(read_source(&write_lines(&dir, &lines)).unwrap(), None);
        assert_eq!(read_source(&write_lines(&dir, &v2(&lines))).unwrap(), None);
    }
}

/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: completed Native and legacy sources retain a user's mid-turn correction.
#[test]
fn user_steering_corrections_remain_in_extraction_input() {
    let dir = TempDir::new().unwrap();
    let mut original = legacy(dir.path());
    original[2]["Item"]["item"]["input_items"][0]["UserMessage"]["text"] = json!("I prefer tabs");
    original[2]["Item"]["item"]["output_items"][0]["AgentMessage"]["text"] = json!("Okay");
    for native in [false, true] {
        let baseline = if native {
            v2(&original)
        } else {
            original.clone()
        };
        let mut expected = read_source(&write_lines(&dir, &baseline)).unwrap().unwrap();
        let mut corrected = original.clone();
        corrected[2]["Item"]["item"]["input_items"]
            .as_array_mut()
            .unwrap()
            .push(json!({"SteerInput":{"text":"Correction: I prefer spaces"}}));
        let lines = if native { v2(&corrected) } else { corrected };
        let steering_id = if native {
            ItemId::from_string(
                lines
                    .iter()
                    .find(|line| line["kind"] == "item" && line["item"]["item"]["entry"] == "steer")
                    .unwrap()["item"]["id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            )
        } else {
            ItemId::from_string(ITEM.into())
        };
        if native {
            expected.messages.last_mut().unwrap().item_id = ItemId::from_string(
                lines
                    .iter()
                    .find(|line| {
                        line["kind"] == "item" && line["item"]["item"]["type"] == "assistantMessage"
                    })
                    .unwrap()["item"]["id"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
        }
        expected.messages.insert(
            1,
            SourceMessage {
                turn_id: TurnId::from_string(TURN.into()),
                item_id: steering_id,
                observed_at: "2026-07-01T12:00:11Z".parse().unwrap(),
                role: "user".into(),
                text: "Correction: I prefer spaces".into(),
            },
        );
        let source = read_source(&write_lines(&dir, &lines)).unwrap().unwrap();
        expected.watermark = source.watermark.clone();
        assert_eq!(source, expected);
    }
}
