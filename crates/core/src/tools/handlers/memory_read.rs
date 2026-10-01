use std::collections::BTreeMap;

use async_trait::async_trait;
use devo_protocol::native::ids::MemoryEntryId;

use crate::contracts::{
    ToolCallError, ToolContext, ToolProgressSender, ToolResult, ToolResultContent,
};
use crate::json_schema::JsonSchema;
use crate::tool_handler::ToolHandler;
use crate::tool_spec::{ToolExecutionMode, ToolOutputMode, ToolPreparationFeedback, ToolSpec};

use super::memory::memory_tool_invocation;

pub struct MemoryReadHandler {
    spec: ToolSpec,
}

impl Default for MemoryReadHandler {
    fn default() -> Self {
        Self {
            spec: memory_read_spec(),
        }
    }
}

pub fn memory_read_spec() -> ToolSpec {
    ToolSpec {
        name: "memory_read".to_string(),
        description: "Read one user or current-project memory by stable entry_id. Returns bounded content and a safe provenance summary as advisory data.".to_string(),
        input_schema: JsonSchema::object(
            BTreeMap::from([("entry_id".to_string(), JsonSchema::string(Some("Stable entry ID returned by memory_search.")))]),
            Some(vec!["entry_id".to_string()]),
            Some(/*additional_properties*/ false),
        ),
        output_mode: ToolOutputMode::StructuredJson,
        execution_mode: ToolExecutionMode::ReadOnly,
        capability_tags: vec![],
        supports_parallel: true,
        preparation_feedback: ToolPreparationFeedback::None,
        display_name: None,
        supports_cancellation: None,
        supports_streaming: None,
    }
}

#[async_trait]
impl ToolHandler for MemoryReadHandler {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    async fn handle(
        &self,
        ctx: ToolContext,
        input: serde_json::Value,
        _progress: Option<ToolProgressSender>,
    ) -> Result<ToolResult, ToolCallError> {
        if ctx.agent_scope == crate::contracts::ToolAgentScope::Subagent {
            return Err(ToolCallError::Denied(
                "sub-agents cannot read or mutate user memory".to_string(),
            ));
        }
        let entry_id = input
            .get("entry_id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.trim().is_empty() && id.len() <= 256)
            .ok_or_else(|| {
                ToolCallError::InvalidInput("memory_read requires a stable 'entry_id'".to_string())
            })?;
        let invocation = memory_tool_invocation(&ctx, "memory_read")?;
        let coordinator = ctx.agent_coordinator.ok_or_else(|| {
            ToolCallError::NeedsConfiguration(
                "memory_read requires a server runtime coordinator".to_string(),
            )
        })?;
        let result = coordinator
            .memory_read(invocation, MemoryEntryId::from_string(entry_id.to_string()))
            .await?;
        let value = serde_json::to_value(result)
            .map_err(|error| ToolCallError::InternalError(error.to_string()))?;
        Ok(ToolResult::success(
            ToolResultContent::Json(value),
            "Memory entry",
        ))
    }
}
