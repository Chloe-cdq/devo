use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use devo_protocol::native::ids::ItemId;
use devo_protocol::native::rpc_memory::MemoryKind;
use devo_protocol::native::rpc_memory::MemoryRememberParams;
use devo_protocol::native::rpc_memory::MemoryScope;
use serde_json::json;

use crate::contracts::ToolResultContent;
use crate::contracts::{ToolCallError, ToolContext, ToolProgressSender, ToolResult};
use crate::json_schema::JsonSchema;
use crate::tool_handler::ToolHandler;
use crate::tool_spec::ToolExecutionMode;
use crate::tool_spec::ToolOutputMode;
use crate::tool_spec::ToolPreparationFeedback;
use crate::tool_spec::ToolSpec;

/// Built-in root-agent action for explicitly persisting a user memory.
pub struct MemoryRememberHandler {
    spec: ToolSpec,
}

impl Default for MemoryRememberHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryRememberHandler {
    pub fn new() -> Self {
        Self {
            spec: memory_remember_spec(),
        }
    }
}

pub fn memory_remember_spec() -> ToolSpec {
    ToolSpec {
        name: "memory_remember".to_string(),
        description: "Remember an explicit user preference, fact, feedback, or reference. Only call this when the current user message clearly asks you to remember something; the server binds it to that message.".to_string(),
        input_schema: JsonSchema::object(
            BTreeMap::from([
                (
                    "text".to_string(),
                    JsonSchema::string(Some("The concise memory text to store.")),
                ),
                (
                    "source_user_item_id".to_string(),
                    JsonSchema::string(Some("The item id of the current user message that explicitly requested this memory.")),
                ),
                (
                    "scope".to_string(),
                    JsonSchema {
                        enum_values: Some(vec![json!("user")]),
                        ..JsonSchema::string(Some("Memory scope. Only user is accepted."))
                    },
                ),
                (
                    "kind".to_string(),
                    JsonSchema {
                        enum_values: Some(vec![
                            json!("preference"),
                            json!("feedback"),
                            json!("fact"),
                            json!("reference"),
                        ]),
                        ..JsonSchema::string(Some("Optional semantic kind; inferred when omitted."))
                    },
                ),
            ]),
            Some(vec!["text".to_string()]),
            Some(false),
        ),
        output_mode: ToolOutputMode::StructuredJson,
        execution_mode: ToolExecutionMode::Mutating,
        capability_tags: vec![],
        supports_parallel: false,
        preparation_feedback: ToolPreparationFeedback::None,
        display_name: None,
        supports_cancellation: None,
        supports_streaming: None,
    }
}

#[async_trait]
impl ToolHandler for MemoryRememberHandler {
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
        let params = parse_memory_remember_input(&input, ctx.current_user_item_id.as_deref())?;
        let turn_id = ctx.turn_id.ok_or_else(|| {
            ToolCallError::InvalidInput(
                "memory_remember requires an active turn with a current user message".to_string(),
            )
        })?;
        let coordinator = ctx.agent_coordinator.ok_or_else(|| {
            ToolCallError::NeedsConfiguration(
                "memory_remember requires a server runtime coordinator".to_string(),
            )
        })?;
        let entry = Arc::clone(&coordinator)
            .memory_remember(ctx.session_id, turn_id, params)
            .await?;
        let value = serde_json::to_value(entry)
            .map_err(|error| ToolCallError::InternalError(error.to_string()))?;
        Ok(ToolResult::success(
            ToolResultContent::Json(value),
            "User memory remembered",
        ))
    }
}

fn parse_memory_remember_input(
    input: &serde_json::Value,
    fallback_source_user_item_id: Option<&str>,
) -> Result<MemoryRememberParams, ToolCallError> {
    let text = input
        .get("text")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ToolCallError::InvalidInput("missing 'text' field".to_string()))?;
    let source_user_item_id = input
        .get("source_user_item_id")
        .or_else(|| input.get("sourceUserItemId"))
        .and_then(serde_json::Value::as_str)
        .or(fallback_source_user_item_id)
        .ok_or_else(|| {
            ToolCallError::InvalidInput(
                "memory_remember requires the current user message context".to_string(),
            )
        })?;
    let scope = match input
        .get("scope")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("user")
    {
        "user" => MemoryScope::User,
        _ => {
            return Err(ToolCallError::InvalidInput(
                "memory_remember only accepts User scope".to_string(),
            ));
        }
    };
    let kind = match input.get("kind").and_then(serde_json::Value::as_str) {
        None => None,
        Some("preference") => Some(MemoryKind::Preference),
        Some("feedback") => Some(MemoryKind::Feedback),
        Some("fact") => Some(MemoryKind::Fact),
        Some("reference") => Some(MemoryKind::Reference),
        Some(_) => {
            return Err(ToolCallError::InvalidInput(
                "memory_remember received an unsupported kind".to_string(),
            ));
        }
    };
    Ok(MemoryRememberParams {
        text: text.to_string(),
        scope,
        kind,
        source_user_item_id: ItemId::from_string(source_user_item_id.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_binds_to_current_user_item_id_without_model_required_field() {
        let schema = memory_remember_spec().input_schema;
        assert_eq!(schema.required, Some(vec!["text".to_string()]));
        let parsed = parse_memory_remember_input(
            &serde_json::json!({"text": "I prefer tabs"}),
            Some("item-current"),
        )
        .expect("server context supplies source item");
        assert_eq!(parsed.source_user_item_id.to_string(), "item-current");
        assert_eq!(
            devo_protocol::native::rpc_memory::MemoryScope::default(),
            devo_protocol::native::rpc_memory::MemoryScope::User
        );
        let _ = devo_protocol::native::rpc_memory::MemoryKind::Fact;
    }
}
