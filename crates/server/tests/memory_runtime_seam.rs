use std::fs;
use std::path::Path;

use devo_core::MemoryConfig;
use devo_core::SessionId;
use devo_protocol::native::page::Page;
use devo_protocol::native::rpc_memory::MemoryProvenance;
use devo_server::memory::{
    MemoryCommand, MemoryCommandResult, MemoryError, MemoryRuntime, ProjectMemoryOperation,
    ProjectMemorySession, ProjectMemorySessionActivity,
};
use pretty_assertions::assert_eq;
use tempfile::TempDir;

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3, DD-13
/// Verifies: Project identity ambiguity is resolved behind execute_command.
#[tokio::test]
async fn project_command_rejects_unrelated_session_candidates() {
    let data_root = TempDir::new().expect("memory data root");
    let project_a = data_root.path().join("project-a");
    let project_b = data_root.path().join("project-b");
    fs::create_dir_all(project_a.join(".git")).expect("create project A repository");
    fs::create_dir_all(project_b.join(".git")).expect("create project B repository");
    let runtime = MemoryRuntime::open(
        data_root.path().join("memory"),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("open enabled memory runtime");

    let error = runtime
        .execute_command(MemoryCommand::Project {
            candidates: vec![
                ProjectMemorySession {
                    session_id: SessionId::new(),
                    workspace_root: Some(project_a.clone()),
                    activity: ProjectMemorySessionActivity::Active,
                },
                ProjectMemorySession {
                    session_id: SessionId::new(),
                    workspace_root: Some(project_b),
                    activity: ProjectMemorySessionActivity::Inactive,
                },
            ],
            operation: ProjectMemoryOperation::Remember {
                text: "the repository uses Rust".into(),
                kind: None,
                source_user_item_id: Some("item-a".into()),
                source_session_id: None,
                source_turn_id: Some("turn-a".into()),
            },
        })
        .await
        .expect_err("unrelated project candidates must be ambiguous");

    assert!(matches!(error, MemoryError::AmbiguousProjectScope));
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-3, DD-13
/// Verifies: Project command selection prefers the active linked-worktree Session.
#[tokio::test]
async fn project_command_uses_active_same_repository_session_for_provenance() {
    let data_root = TempDir::new().expect("memory data root");
    let repository_root = data_root.path().join("repository");
    let common_git_dir = repository_root.join(".git");
    let linked_root = data_root.path().join("linked-worktree");
    let linked_git_dir = common_git_dir.join("worktrees").join("linked");
    fs::create_dir_all(&common_git_dir).expect("create repository git directory");
    fs::create_dir_all(&linked_git_dir).expect("create linked git directory");
    fs::create_dir_all(&linked_root).expect("create linked worktree");
    let common_dir = Path::new("..").join("..");
    fs::write(
        linked_git_dir.join("commondir"),
        format!("{}\n", common_dir.display()),
    )
    .expect("write common dir");
    fs::write(
        linked_root.join(".git"),
        format!("gitdir: {}\n", linked_git_dir.display()),
    )
    .expect("write linked worktree git file");
    let runtime = MemoryRuntime::open(
        data_root.path().join("memory"),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("open enabled memory runtime");

    let main_session_id = SessionId::new();
    let linked_session_id = SessionId::new();
    let result = runtime
        .execute_command(MemoryCommand::Project {
            candidates: vec![
                ProjectMemorySession {
                    session_id: main_session_id,
                    workspace_root: Some(repository_root.clone()),
                    activity: ProjectMemorySessionActivity::Inactive,
                },
                ProjectMemorySession {
                    session_id: linked_session_id,
                    workspace_root: Some(linked_root),
                    activity: ProjectMemorySessionActivity::Active,
                },
            ],
            operation: ProjectMemoryOperation::Remember {
                text: "the repository uses Rust".into(),
                kind: None,
                source_user_item_id: None,
                source_session_id: None,
                source_turn_id: None,
            },
        })
        .await
        .expect("same-repository candidates should resolve");
    let entry = match result {
        MemoryCommandResult::Remember(entry) => entry,
        MemoryCommandResult::Status(_) | MemoryCommandResult::List(_) => {
            panic!("unexpected Project remember result")
        }
    };

    assert_eq!(
        entry.provenance,
        vec![MemoryProvenance {
            source_session_id: Some(linked_session_id.to_string()),
            source_turn_id: None,
            source_user_item_id: None,
        }]
    );
}

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-2, DD-13
/// Verifies: global disable dominates Project selector validation.
#[tokio::test]
async fn disabled_project_commands_do_not_resolve_session_candidates() {
    let data_root = TempDir::new().expect("memory data root");
    let runtime = MemoryRuntime::open(data_root.path().join("memory"), MemoryConfig::default())
        .expect("open disabled memory runtime");

    let listed = runtime
        .execute_command(MemoryCommand::Project {
            candidates: Vec::new(),
            operation: ProjectMemoryOperation::List {
                kind: None,
                state: None,
                origin: None,
                text: None,
                cursor: None,
                limit: None,
            },
        })
        .await
        .expect("disabled Project list should be empty");
    assert_eq!(
        listed,
        MemoryCommandResult::List(Page {
            data: Vec::new(),
            next_cursor: None,
        })
    );

    let error = runtime
        .execute_command(MemoryCommand::Project {
            candidates: Vec::new(),
            operation: ProjectMemoryOperation::Remember {
                text: "the repository uses Rust".into(),
                kind: None,
                source_user_item_id: None,
                source_session_id: None,
                source_turn_id: None,
            },
        })
        .await
        .expect_err("disabled Project remember should be rejected by the global gate");
    assert!(matches!(error, MemoryError::Disabled));
}
