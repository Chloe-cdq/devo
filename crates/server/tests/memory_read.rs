use std::path::Path;

use anyhow::Result;
use devo_core::MemoryConfig;
use devo_protocol::SessionId;
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::{MemoryKind, MemoryReadEntry, MemoryScope, MemoryState};
use devo_server::memory::{
    MemoryCommand, MemoryCommandResult, MemoryError, MemoryRememberRequest, MemoryRuntime,
    MemorySourceContext, ReadMemoryRequest,
};
use pretty_assertions::assert_eq;

async fn remember(
    memory: &MemoryRuntime,
    workspace: &Path,
    scope: MemoryScope,
    text: &str,
) -> Result<MemoryEntryId> {
    let MemoryCommandResult::Remember(entry) = memory
        .execute_command(MemoryCommand::Remember(MemoryRememberRequest {
            text: text.into(),
            scope,
            kind: Some(MemoryKind::Fact),
            source: MemorySourceContext {
                user_item_id: None,
                session_id: SessionId::new(),
                turn_id: None,
                workspace_root: workspace.to_path_buf(),
            },
        }))
        .await?
    else {
        panic!("expected remembered entry");
    };
    Ok(entry.entry_id)
}

/// Trace: L2-DES-MEM-001 Rev 4 Built-in Agent Tools, Privacy and Authority
/// Verifies: Unicode entry bodies are bounded and raw evidence is summarized.
#[tokio::test]
async fn read_bounds_unicode_body_and_summarizes_provenance() -> Result<()> {
    let data = tempfile::tempdir()?;
    let memory = MemoryRuntime::open(
        data.path().join("memory"),
        MemoryConfig {
            enabled: true,
            ..Default::default()
        },
    )?;
    let id = remember(&memory, data.path(), MemoryScope::User, &"界".repeat(4001)).await?;
    let result = memory
        .execute_command(MemoryCommand::Read(ReadMemoryRequest {
            entry_id: id.clone(),
            workspace_root: data.path().to_path_buf(),
        }))
        .await?;
    assert_eq!(
        result,
        MemoryCommandResult::Read(MemoryReadEntry {
            entry_id: id,
            scope: MemoryScope::User,
            kind: MemoryKind::Fact,
            state: MemoryState::Active,
            body: format!("{}…", "界".repeat(4000)),
            source_summary: "Explicit user memory (1 source)".into(),
        })
    );
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 Built-in Agent Tools, DD-3
/// Verifies: stable IDs cannot read a foreign project, and absence shares its safe error.
#[tokio::test]
async fn read_enforces_project_identity_and_hides_foreign_entries() -> Result<()> {
    let data = tempfile::tempdir()?;
    let project = data.path().join("project");
    let other = data.path().join("other");
    std::fs::create_dir(&project)?;
    std::fs::create_dir(&other)?;
    let memory = MemoryRuntime::open(
        data.path().join("memory"),
        MemoryConfig {
            enabled: true,
            ..Default::default()
        },
    )?;
    let id = remember(&memory, &project, MemoryScope::Project, "Use tabs").await?;
    let result = memory
        .execute_command(MemoryCommand::Read(ReadMemoryRequest {
            entry_id: id.clone(),
            workspace_root: project,
        }))
        .await?;
    assert_eq!(
        result,
        MemoryCommandResult::Read(MemoryReadEntry {
            entry_id: id.clone(),
            scope: MemoryScope::Project,
            kind: MemoryKind::Fact,
            state: MemoryState::Active,
            body: "Use tabs".into(),
            source_summary: "Explicit user memory (1 source)".into(),
        })
    );
    for entry_id in [id, "unknown-entry".into()] {
        let error = memory
            .execute_command(MemoryCommand::Read(ReadMemoryRequest {
                entry_id,
                workspace_root: other.clone(),
            }))
            .await
            .expect_err("foreign or missing entry");
        assert_eq!(
            error.to_string(),
            "invalid memory request: memory entry is unavailable"
        );
    }
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 Privacy and Authority
/// Verifies: a secret beyond the displayed body boundary still prevents projection.
#[tokio::test]
async fn read_rejects_secret_bearing_stored_content() -> Result<()> {
    let data = tempfile::tempdir()?;
    let memory = MemoryRuntime::open(
        data.path().join("memory"),
        MemoryConfig {
            enabled: true,
            ..Default::default()
        },
    )?;
    let id = remember(&memory, data.path(), MemoryScope::User, "Use tabs").await?;
    let db = rusqlite::Connection::open(data.path().join("memory/memory.sqlite3"))?;
    db.execute(
        "UPDATE memory_entries SET body = ?1 WHERE entry_id = ?2",
        rusqlite::params![
            format!("{} api_key=private-value", "safe ".repeat(1000)),
            id.as_str(),
        ],
    )?;
    let result = memory
        .execute_command(MemoryCommand::Read(ReadMemoryRequest {
            entry_id: id,
            workspace_root: data.path().to_path_buf(),
        }))
        .await;
    assert!(matches!(result, Err(MemoryError::SecretContentRejected)));
    Ok(())
}

/// Trace: L2-DES-MEM-001 Rev 4 DD-2
/// Verifies: global disable dominates on-demand read regardless of the stable ID.
#[tokio::test]
async fn disabled_memory_rejects_read() -> Result<()> {
    let data = tempfile::tempdir()?;
    let memory = MemoryRuntime::open(data.path().join("memory"), MemoryConfig::default())?;
    let result = memory
        .execute_command(MemoryCommand::Read(ReadMemoryRequest {
            entry_id: "any-entry".into(),
            workspace_root: data.path().to_path_buf(),
        }))
        .await;
    assert!(matches!(result, Err(MemoryError::Disabled)));
    Ok(())
}
