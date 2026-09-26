use devo_core::tools::MemoryToolInvocation;

use super::*;
use crate::runtime::memory_forget_authorization::{
    MemoryForgetExecutionError, complete_forget_execution,
};

struct MemoryMutationContext {
    memory: Arc<crate::memory::MemoryRuntime>,
    source: crate::memory::MemorySourceContext,
}

impl MemoryMutationContext {
    async fn new(
        runtime: &ServerRuntime,
        invocation: &MemoryToolInvocation,
    ) -> Result<Self, ToolCallError> {
        let memory = runtime.memory.clone().ok_or_else(|| {
            ToolCallError::NeedsConfiguration("memory runtime is unavailable".to_string())
        })?;
        let summary = runtime
            .session_summary_snapshot(invocation.session_id)
            .await
            .ok_or_else(|| ToolCallError::InvalidInput("session not found".to_string()))?;
        if summary.parent_session_id.is_some() {
            return Err(ToolCallError::Denied(
                "sub-agents cannot mutate user memory".to_string(),
            ));
        }
        Ok(Self {
            memory,
            source: crate::memory::MemorySourceContext {
                user_item_id: Some(invocation.user_item_id.clone()),
                session_id: invocation.session_id,
                turn_id: Some(invocation.turn_id),
                workspace_root: summary.cwd,
            },
        })
    }
}

pub(super) async fn remember(
    runtime: Arc<ServerRuntime>,
    invocation: MemoryToolInvocation,
    params: devo_protocol::native::rpc_memory::MemoryRememberParams,
) -> Result<devo_protocol::native::rpc_memory::MemoryEntry, ToolCallError> {
    if params.source_user_item_id.as_ref() != Some(&invocation.user_item_id) {
        return Err(ToolCallError::InvalidInput(
            "memory_remember source must match the current user message context".to_string(),
        ));
    }
    runtime
        .current_user_item_text(
            invocation.session_id,
            invocation.turn_id,
            &invocation.user_item_id,
        )
        .await
        .map_err(|error| ToolCallError::InvalidInput(error.to_string()))?;
    let context = MemoryMutationContext::new(&runtime, &invocation).await?;
    let result = context
        .memory
        .execute_command(crate::memory::MemoryCommand::Remember(
            crate::memory::MemoryRememberRequest {
                text: params.text,
                scope: params.scope,
                kind: params.kind,
                source: context.source,
            },
        ))
        .await
        .map_err(memory_tool_error)?;
    match result {
        crate::memory::MemoryCommandResult::Remember(entry) => Ok(entry),
        crate::memory::MemoryCommandResult::Status(_)
        | crate::memory::MemoryCommandResult::PreparedForget(_)
        | crate::memory::MemoryCommandResult::Forget(_)
        | crate::memory::MemoryCommandResult::List(_)
        | crate::memory::MemoryCommandResult::Search(_) => Err(ToolCallError::InternalError(
            "memory_remember returned an unexpected result".to_string(),
        )),
    }
}

pub(super) async fn forget(
    runtime: Arc<ServerRuntime>,
    invocation: MemoryToolInvocation,
    params: devo_protocol::native::rpc_memory::MemoryForgetParams,
) -> Result<devo_protocol::native::rpc_memory::MemoryForgetResult, ToolCallError> {
    if params.source_user_item_id.as_ref() != Some(&invocation.user_item_id) {
        return Err(ToolCallError::InvalidInput(
            "memory_forget source must match the current user message context".to_string(),
        ));
    }
    let entry_id = match crate::memory::MemoryForgetSelector::from_params(&params)
        .map_err(|message| ToolCallError::InvalidInput(message.to_string()))?
    {
        crate::memory::MemoryForgetSelector::EntryId(entry_id) => entry_id,
        crate::memory::MemoryForgetSelector::Text(_) => {
            return Err(ToolCallError::InvalidInput(
                "root-agent memory_forget requires an exact stable ID".to_string(),
            ));
        }
    };
    let user_text = runtime
        .current_user_item_text(
            invocation.session_id,
            invocation.turn_id,
            &invocation.user_item_id,
        )
        .await
        .map_err(|error| ToolCallError::InvalidInput(error.to_string()))?;
    let context = MemoryMutationContext::new(&runtime, &invocation).await?;
    let selector = crate::memory::MemoryForgetSelector::EntryId(entry_id.clone());
    let source_session_id = context.source.session_id;
    let prepared = crate::runtime::memory_forget_preparation::prepare_forget(
        &runtime,
        &context.memory,
        crate::memory::MemoryForgetRequest {
            selector,
            scope: params.scope,
            source: crate::memory::MemoryForgetSource {
                bound_session_id: Some(source_session_id),
                user_session_id: Some(source_session_id),
                sessions: vec![crate::memory::ProjectMemorySession {
                    session_id: source_session_id,
                    workspace_root: Some(context.source.workspace_root),
                    activity: crate::memory::ProjectMemorySessionActivity::Active,
                }],
            },
        },
    )
    .await
    .map_err(memory_tool_error)?;
    let reservation = runtime.memory_forget_coordinator.authorize_agent(
        &invocation,
        &user_text,
        &entry_id,
        prepared.scope(),
    )?;
    let execution = runtime
        .deps
        .memory_command_executor
        .execute(
            &context.memory,
            crate::memory::MemoryCommand::Forget(prepared),
        )
        .await;
    match complete_forget_execution(reservation, execution) {
        Ok(result) => Ok(result),
        Err(MemoryForgetExecutionError::Coordinator(error)) => Err(error),
        Err(MemoryForgetExecutionError::Storage(error)) => Err(memory_tool_error(error)),
        Err(MemoryForgetExecutionError::Projection(error)) => Err(ToolCallError::InternalError(
            format!("memory forget committed but projection refresh failed: {error}"),
        )),
        Err(MemoryForgetExecutionError::UnexpectedResult) => Err(ToolCallError::InternalError(
            "memory_forget returned an unexpected result".to_string(),
        )),
    }
}

pub(super) async fn search(
    runtime: Arc<ServerRuntime>,
    invocation: MemoryToolInvocation,
    params: devo_protocol::native::rpc_memory::MemorySearchParams,
) -> Result<devo_protocol::native::rpc_memory::MemorySearchResult, ToolCallError> {
    runtime
        .current_user_item_text(
            invocation.session_id,
            invocation.turn_id,
            &invocation.user_item_id,
        )
        .await
        .map_err(|error| ToolCallError::InvalidInput(error.to_string()))?;
    let memory = runtime.memory.clone().ok_or_else(|| {
        ToolCallError::NeedsConfiguration("memory runtime is unavailable".to_string())
    })?;
    let summary = runtime
        .session_summary_snapshot(invocation.session_id)
        .await
        .ok_or_else(|| ToolCallError::InvalidInput("session not found".to_string()))?;
    let scope = params.scope.unwrap_or_default();
    let workspace_root = if scope == devo_protocol::native::rpc_memory::MemoryScope::Project {
        summary.cwd
    } else {
        std::path::PathBuf::new()
    };
    let search_epoch = runtime.memory_forget_coordinator.begin_search()?;
    let result = runtime
        .deps
        .memory_command_executor
        .execute(
            &memory,
            crate::memory::MemoryCommand::Search(crate::memory::SearchMemoryRequest {
                query: params.query,
                scope,
                kind: params.kind,
                state: params.state,
                workspace_root,
            }),
        )
        .await
        .map_err(memory_tool_error)?;
    let result = match result {
        crate::memory::MemoryCommandResult::Search(result) => result,
        crate::memory::MemoryCommandResult::Status(_)
        | crate::memory::MemoryCommandResult::Remember(_)
        | crate::memory::MemoryCommandResult::PreparedForget(_)
        | crate::memory::MemoryCommandResult::Forget(_)
        | crate::memory::MemoryCommandResult::List(_) => {
            return Err(ToolCallError::InternalError(
                "memory_search returned an unexpected result".to_string(),
            ));
        }
    };
    runtime.memory_forget_coordinator.record_search_snapshot(
        &invocation,
        &result.data,
        search_epoch,
    )?;
    Ok(result)
}

fn memory_tool_error(error: crate::memory::MemoryError) -> ToolCallError {
    match error {
        crate::memory::MemoryError::InvalidRequest(message) => ToolCallError::InvalidInput(message),
        crate::memory::MemoryError::SecretContentRejected => {
            ToolCallError::Denied("memory content was rejected for safety".to_string())
        }
        crate::memory::MemoryError::Disabled => {
            ToolCallError::NeedsConfiguration("memory is disabled".to_string())
        }
        crate::memory::MemoryError::AmbiguousProjectScope => ToolCallError::InvalidInput(
            "memory operation has ambiguous Native Session selectors".to_string(),
        ),
        crate::memory::MemoryError::ProjectSessionRequired => ToolCallError::InvalidInput(
            "Project memory requires a session-bound connection".to_string(),
        ),
        crate::memory::MemoryError::ProjectSessionUnavailable => ToolCallError::InvalidInput(
            "Project memory requires a session with a workspace root".to_string(),
        ),
        crate::memory::MemoryError::Directory(_)
        | crate::memory::MemoryError::Database(_)
        | crate::memory::MemoryError::LockPoisoned
        | crate::memory::MemoryError::InvalidCount(_)
        | crate::memory::MemoryError::InvalidTimestamp(_)
        | crate::memory::MemoryError::ProjectIdentity(_)
        | crate::memory::MemoryError::InvalidStoredValue(_)
        | crate::memory::MemoryError::ForgetCommitted { .. } => {
            ToolCallError::InternalError("memory operation is unavailable".to_string())
        }
    }
}
