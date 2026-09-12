use std::sync::Arc;

use async_trait::async_trait;
use devo_protocol::native::ids::ItemId;
use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_protocol::native::rpc_memory::MemoryForgetParams;
use devo_protocol::native::rpc_memory::MemoryForgetResult;
use devo_protocol::native::rpc_memory::MemoryRememberParams;
use devo_protocol::native::rpc_memory::MemorySearchParams;
use devo_protocol::native::rpc_memory::MemorySearchResult;
use devo_protocol::{
    AgentInfo, AgentListParams, AgentMessageParams, AgentMessageResult, AwaitTaskParams,
    AwaitTaskResult, CancelTaskParams, CancelTaskResult, CloseAgentParams, CloseAgentResult,
    ListTasksParams, ListTasksResult, RequestUserInputArgs, RequestUserInputResponse, SessionId,
    SpawnAgentParams, SpawnAgentResult, TurnId, WaitAgentParams, WaitAgentResult,
};
use serde_json::Value;

use crate::contracts::ToolCallError;

/// Server-bound identity of the user turn invoking a root-agent memory tool.
///
/// Coordinators must treat these IDs as trusted execution context rather than
/// accepting model-selected source identities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryToolInvocation {
    /// Session whose active root turn is invoking the tool.
    pub session_id: SessionId,
    /// Active turn that owns the invocation.
    pub turn_id: TurnId,
    /// Server-bound user item that started the active turn.
    pub user_item_id: ItemId,
}

/// Runtime bridge used by built-in agent tools to coordinate child agents.
///
/// Implementations own session-tree state, mailboxes, persistence, and turn
/// execution. Tool handlers should validate model-facing input, fill in the
/// current session from `ToolContext`, and delegate to this trait.
#[async_trait]
pub trait AgentToolCoordinator: Send + Sync {
    async fn spawn_agent(
        self: Arc<Self>,
        params: SpawnAgentParams,
    ) -> Result<SpawnAgentResult, ToolCallError>;

    async fn send_message(
        self: Arc<Self>,
        params: AgentMessageParams,
    ) -> Result<AgentMessageResult, ToolCallError>;

    async fn wait_agent(
        self: Arc<Self>,
        params: WaitAgentParams,
    ) -> Result<WaitAgentResult, ToolCallError>;

    async fn list_agents(
        self: Arc<Self>,
        params: AgentListParams,
    ) -> Result<Vec<AgentInfo>, ToolCallError>;

    async fn close_agent(
        self: Arc<Self>,
        params: CloseAgentParams,
    ) -> Result<CloseAgentResult, ToolCallError>;

    async fn await_task(
        self: Arc<Self>,
        _params: AwaitTaskParams,
    ) -> Result<AwaitTaskResult, ToolCallError> {
        Err(ToolCallError::ExecutionFailed(
            "await_task is unavailable in this runtime".to_string(),
        ))
    }

    async fn list_tasks(
        self: Arc<Self>,
        _params: ListTasksParams,
    ) -> Result<ListTasksResult, ToolCallError> {
        Err(ToolCallError::ExecutionFailed(
            "list_tasks is unavailable in this runtime".to_string(),
        ))
    }

    async fn cancel_task(
        self: Arc<Self>,
        _params: CancelTaskParams,
    ) -> Result<CancelTaskResult, ToolCallError> {
        Err(ToolCallError::ExecutionFailed(
            "cancel_task is unavailable in this runtime".to_string(),
        ))
    }

    async fn request_user_input(
        self: Arc<Self>,
        _session_id: String,
        _turn_id: String,
        _tool_call_id: String,
        _args: RequestUserInputArgs,
    ) -> Result<RequestUserInputResponse, ToolCallError> {
        Err(ToolCallError::ExecutionFailed(
            "request_user_input is unavailable in this runtime".to_string(),
        ))
    }

    async fn update_goal(
        self: Arc<Self>,
        _session_id: String,
        _status: String,
    ) -> Result<Value, ToolCallError> {
        Err(ToolCallError::ExecutionFailed(
            "update_goal is unavailable in this runtime".to_string(),
        ))
    }

    /// Commits one root-agent memory request after the runtime validates that
    /// its source item is the current user message.
    async fn memory_remember(
        self: Arc<Self>,
        _invocation: MemoryToolInvocation,
        _params: MemoryRememberParams,
    ) -> Result<MemoryEntry, ToolCallError> {
        Err(ToolCallError::ExecutionFailed(
            "memory_remember is unavailable in this runtime".to_string(),
        ))
    }

    /// Retires one root-agent memory identity after the runtime validates the
    /// current user's explicit forget request.
    async fn memory_forget(
        self: Arc<Self>,
        _invocation: MemoryToolInvocation,
        _params: MemoryForgetParams,
    ) -> Result<MemoryForgetResult, ToolCallError> {
        Err(ToolCallError::ExecutionFailed(
            "memory_forget is unavailable in this runtime".to_string(),
        ))
    }

    /// Searches bounded root-agent memory candidates before an exact-ID
    /// mutation is requested.
    async fn memory_search(
        self: Arc<Self>,
        _invocation: MemoryToolInvocation,
        _params: MemorySearchParams,
    ) -> Result<MemorySearchResult, ToolCallError> {
        Err(ToolCallError::ExecutionFailed(
            "memory_search is unavailable in this runtime".to_string(),
        ))
    }
}
