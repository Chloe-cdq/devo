use std::path::Path;
use std::path::PathBuf;

use devo_core::MemoryConfig;
use devo_protocol::native::rpc_memory::MemoryKind;
use devo_protocol::native::rpc_memory::MemoryScope;

use super::test_support::test_source;
use super::{
    MemoryCommand, MemoryCommandResult, MemoryError, MemoryForgetRequest, MemoryForgetSelector,
    MemoryForgetSource, MemoryRememberRequest, MemoryRuntime, PreparedMemoryForgetRequest,
    ProjectMemorySession, ProjectMemorySessionActivity,
};

pub fn forget_request(selector: MemoryForgetSelector) -> MemoryForgetRequest {
    let source = test_source(
        /*user_item_id*/ None,
        "session-1",
        /*turn_id*/ None,
        PathBuf::new(),
    );
    MemoryForgetRequest {
        selector,
        scope: MemoryScope::User,
        source: MemoryForgetSource {
            bound_session_id: Some(source.session_id),
            user_session_id: Some(source.session_id),
            sessions: vec![ProjectMemorySession {
                session_id: source.session_id,
                workspace_root: Some(source.workspace_root),
                activity: ProjectMemorySessionActivity::Active,
            }],
        },
    }
}

pub async fn prepare_forget(
    runtime: &MemoryRuntime,
    request: MemoryForgetRequest,
) -> Result<PreparedMemoryForgetRequest, MemoryError> {
    match runtime
        .execute_command(MemoryCommand::PrepareForget(request))
        .await?
    {
        MemoryCommandResult::PreparedForget(prepared) => Ok(prepared),
        MemoryCommandResult::Status(_)
        | MemoryCommandResult::Remember(_)
        | MemoryCommandResult::Forget(_)
        | MemoryCommandResult::List(_) => Err(MemoryError::InvalidStoredValue(
            "forget preparation returned an unexpected result".to_string(),
        )),
    }
}

pub fn remember_request(text: &str) -> MemoryRememberRequest {
    MemoryRememberRequest {
        text: text.to_owned(),
        scope: MemoryScope::User,
        kind: Some(MemoryKind::Preference),
        source: test_source(
            Some("user-item-1"),
            "session-1",
            Some("turn-1"),
            PathBuf::new(),
        ),
    }
}

pub fn open_runtime(root: &Path) -> MemoryRuntime {
    MemoryRuntime::open(
        root.to_path_buf(),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("memory runtime")
}
