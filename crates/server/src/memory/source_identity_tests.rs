use devo_protocol::native::session::SessionSource;
use pretty_assertions::assert_eq;

use super::{MemorySetting, TempDir, legacy, read_source};
use crate::memory::SessionMemorySettings;
use crate::persistence::RolloutStore;

fn write_canonical_source(
    dir: &TempDir,
    source: SessionSource,
) -> (RolloutStore, devo_core::SessionRecord) {
    let fixture = legacy(dir.path());
    let meta: devo_core::SessionRecord =
        serde_json::from_value(fixture[0]["SessionMeta"]["session"].clone()).unwrap();
    let store = RolloutStore::new(dir.path().into(), /*event_log*/ None);
    let record = store.create_session_record(
        meta.id,
        meta.created_at,
        dir.path().into(),
        Vec::new(),
        meta.title,
        meta.model,
        meta.model_binding_id,
        meta.reasoning_effort_selection,
        meta.model_provider,
        /*parent_session_id*/ None,
    );
    store.append_session_meta(&record).unwrap();
    store
        .append_initial_memory_settings_at(
            &record.rollout_path,
            record.id,
            SessionMemorySettings {
                source,
                contribution: MemorySetting::On,
                ..Default::default()
            },
        )
        .unwrap();
    store
        .append_turn(
            &record,
            serde_json::from_value(fixture[1]["Turn"]["turn"].clone()).unwrap(),
        )
        .unwrap();
    store
        .append_item(
            &record,
            serde_json::from_value(fixture[2]["Item"]["item"].clone()).unwrap(),
        )
        .unwrap();
    (store, record)
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-1/DD-2/DD-7.
/// Verifies: the production writer's field-level automation identity prevents passive contribution.
#[test]
fn canonical_source_setting_controls_admission() {
    for (source, expected) in [
        (
            SessionSource::Interactive,
            Some((
                MemorySetting::On,
                vec!["Fix the flaky test".to_string(), "On it.".to_string()],
            )),
        ),
        (SessionSource::Automation, None),
    ] {
        let dir = TempDir::new().unwrap();
        let (_, record) = write_canonical_source(&dir, source);
        let actual = read_source(&record.rollout_path).unwrap().map(|source| {
            (
                source.session_contribution,
                source
                    .messages
                    .into_iter()
                    .map(|message| message.text)
                    .collect(),
            )
        });
        assert_eq!(actual, expected, "{source:?}");
    }
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 DD-1/DD-2/DD-7.
/// Verifies: malformed field-level source identities fail closed rather than defaulting to interactive.
#[test]
fn malformed_source_settings_are_excluded() {
    for value in [
        serde_json::Value::Null,
        serde_json::json!("unknown"),
        serde_json::json!({"source":"interactive"}),
    ] {
        let dir = TempDir::new().unwrap();
        let (store, record) = write_canonical_source(&dir, SessionSource::Interactive);
        store
            .append_session_settings_batch_at(
                &record.rollout_path,
                record.id,
                &[(devo_core::SessionSettingsField::SessionSource, value)],
            )
            .unwrap();
        assert_eq!(read_source(&record.rollout_path).unwrap(), None);
    }
}
