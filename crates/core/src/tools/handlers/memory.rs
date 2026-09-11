use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use devo_protocol::native::ids::ItemId;
use devo_protocol::native::ids::MemoryEntryId;
use devo_protocol::native::rpc_memory::MemoryForgetParams;
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

/// Built-in root-agent action for explicitly persisting User or Project memory.
pub struct MemoryRememberHandler {
    spec: ToolSpec,
}

/// Built-in root-agent action for safely retiring User or Project memory.
pub struct MemoryForgetHandler {
    spec: ToolSpec,
}

impl Default for MemoryForgetHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryForgetHandler {
    pub fn new() -> Self {
        Self {
            spec: memory_forget_spec(),
        }
    }
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
        description: "Remember an explicit user or project preference, fact, feedback, or reference. Only call this when the current user message clearly asks you to remember something; the server binds it to that message.".to_string(),
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
                        enum_values: Some(vec![json!("user"), json!("project")]),
                        ..JsonSchema::string(Some("Memory scope: user or project."))
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
            Some(/*additional_properties*/ false),
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

pub fn memory_forget_spec() -> ToolSpec {
    ToolSpec {
        name: "memory_forget".to_string(),
        description: "Forget one user or project memory by its exact entry_id. Use memory_search first for natural-language requests, then pass the selected stable ID. Only call this when the current user explicitly asks to forget or remove the memory.".to_string(),
        input_schema: JsonSchema::object(
            BTreeMap::from([
                (
                    "entry_id".to_string(),
                    JsonSchema::string(Some("Stable memory entry id to retire exactly.")),
                ),
                (
                    "source_user_item_id".to_string(),
                    JsonSchema::string(Some("The item id of the current user message that explicitly requested forgetting.")),
                ),
            ]),
            Some(Vec::new()),
            Some(/*additional_properties*/ false),
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
            "Memory remembered",
        ))
    }
}

#[async_trait]
impl ToolHandler for MemoryForgetHandler {
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
        let params = parse_memory_forget_input(&input, ctx.current_user_item_id.as_deref())?;
        let turn_id = ctx.turn_id.ok_or_else(|| {
            ToolCallError::InvalidInput(
                "memory_forget requires an active turn with a current user message".to_string(),
            )
        })?;
        let coordinator = ctx.agent_coordinator.ok_or_else(|| {
            ToolCallError::NeedsConfiguration(
                "memory_forget requires a server runtime coordinator".to_string(),
            )
        })?;
        let result = Arc::clone(&coordinator)
            .memory_forget(ctx.session_id, turn_id, params)
            .await?;
        let value = serde_json::to_value(result)
            .map_err(|error| ToolCallError::InternalError(error.to_string()))?;
        Ok(ToolResult::success(
            ToolResultContent::Json(value),
            "Memory forget result",
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
    let input_source_user_item_id = input
        .get("source_user_item_id")
        .or_else(|| input.get("sourceUserItemId"))
        .and_then(serde_json::Value::as_str);
    let source_user_item_id = fallback_source_user_item_id.ok_or_else(|| {
        ToolCallError::InvalidInput(
            "memory_remember requires the current user message context".to_string(),
        )
    })?;
    if input_source_user_item_id.is_some_and(|source| source != source_user_item_id) {
        return Err(ToolCallError::InvalidInput(
            "memory_remember source must match the current user message context".to_string(),
        ));
    }
    let scope = match input
        .get("scope")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("user")
    {
        "user" => MemoryScope::User,
        "project" => MemoryScope::Project,
        _ => {
            return Err(ToolCallError::InvalidInput(
                "memory_remember received an unsupported scope".to_string(),
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
        source_user_item_id: Some(ItemId::from_string(source_user_item_id.to_string())),
    })
}

fn parse_memory_forget_input(
    input: &serde_json::Value,
    fallback_source_user_item_id: Option<&str>,
) -> Result<MemoryForgetParams, ToolCallError> {
    if input.get("text").is_some() || input.get("scope").is_some() {
        return Err(ToolCallError::InvalidInput(
            "memory_forget accepts only 'entry_id' and server-bound source context".to_string(),
        ));
    }
    let entry_id = input
        .get("entry_id")
        .or_else(|| input.get("entryId"))
        .and_then(serde_json::Value::as_str)
        .map(|id| MemoryEntryId::from_string(id.to_string()))
        .ok_or_else(|| {
            ToolCallError::InvalidInput(
                "memory_forget requires a stable 'entry_id'; use memory_search for text selectors"
                    .to_string(),
            )
        })?;
    let input_source_user_item_id = input
        .get("source_user_item_id")
        .or_else(|| input.get("sourceUserItemId"))
        .and_then(serde_json::Value::as_str);
    let source_user_item_id = fallback_source_user_item_id.ok_or_else(|| {
        ToolCallError::InvalidInput(
            "memory_forget requires the current user message context".to_string(),
        )
    })?;
    if input_source_user_item_id.is_some_and(|source| source != source_user_item_id) {
        return Err(ToolCallError::InvalidInput(
            "memory_forget source must match the current user message context".to_string(),
        ));
    }
    Ok(MemoryForgetParams {
        entry_id: Some(entry_id),
        text: None,
        scope: MemoryScope::User,
        source_user_item_id: Some(ItemId::from_string(source_user_item_id.to_string())),
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// Trace: L2-DES-MEM-001
    /// Verifies: the root-agent schema lets the server supply the current user item binding.
    #[test]
    fn schema_binds_to_current_user_item_id_without_model_required_field() {
        let schema = memory_remember_spec().input_schema;
        assert_eq!(schema.required, Some(vec!["text".to_string()]));
        let parsed = parse_memory_remember_input(
            &serde_json::json!({"text": "I prefer tabs"}),
            Some("item-current"),
        )
        .expect("server context supplies source item");
        assert_eq!(
            parsed.source_user_item_id.as_ref().map(ToString::to_string),
            Some("item-current".to_string())
        );
        assert_eq!(parsed.scope, MemoryScope::User);
        assert_eq!(parsed.kind, None);
        let error = parse_memory_remember_input(
            &serde_json::json!({
                "text": "I prefer tabs",
                "sourceUserItemId": "item-other"
            }),
            Some("item-current"),
        )
        .expect_err("the model cannot override the server-bound source item");
        assert_eq!(
            error.to_string(),
            "invalid input: memory_remember source must match the current user message context"
        );
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: the root-agent memory tool exposes and preserves Project scope.
    #[test]
    fn project_memory_tool_input_preserves_project_scope() {
        let schema = memory_remember_spec().input_schema;
        assert_eq!(
            schema
                .properties
                .as_ref()
                .and_then(|properties| properties.get("scope"))
                .and_then(|scope| scope.enum_values.clone()),
            Some(vec![json!("user"), json!("project")])
        );

        let parsed = parse_memory_remember_input(
            &serde_json::json!({
                "text": "the repository uses Rust",
                "scope": "project"
            }),
            Some("item-current"),
        )
        .expect("project scope is valid for explicit memory");
        assert_eq!(parsed.scope, MemoryScope::Project);
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: forgetting accepts one stable ID and preserves the server binding.
    #[test]
    fn forget_tool_parses_exact_selector() {
        let exact = parse_memory_forget_input(
            &serde_json::json!({"entryId": "mem-existing"}),
            Some("item-current"),
        )
        .expect("exact entry selector is valid");
        assert_eq!(
            exact,
            MemoryForgetParams {
                entry_id: Some(MemoryEntryId::from_string("mem-existing".to_string())),
                text: None,
                scope: MemoryScope::User,
                source_user_item_id: Some(ItemId::from_string("item-current".to_string())),
            }
        );
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: the agent mutation tool rejects text selectors and caller-selected scopes.
    #[test]
    fn forget_tool_rejects_non_contract_selectors() {
        let missing_entry_id =
            parse_memory_forget_input(&serde_json::json!({}), Some("item-current"))
                .expect_err("agent forget requires a stable entry ID");
        assert_eq!(
            missing_entry_id.to_string(),
            "invalid input: memory_forget requires a stable 'entry_id'; use memory_search for text selectors"
        );

        for input in [
            serde_json::json!({"text": "old timezone"}),
            serde_json::json!({"entry_id": "mem-existing", "scope": "project"}),
        ] {
            let error = parse_memory_forget_input(&input, Some("item-current"))
                .expect_err("agent forget rejects non-contract selectors");
            assert_eq!(
                error.to_string(),
                "invalid input: memory_forget accepts only 'entry_id' and server-bound source context"
            );
        }
    }

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: the root-agent forget schema exposes only the approved stable-ID mutation surface.
    #[test]
    fn forget_tool_schema_exposes_only_stable_id_selector() {
        let schema = memory_forget_spec().input_schema;
        let properties = schema.properties.expect("forget tool properties");
        assert_eq!(
            properties.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["entry_id", "source_user_item_id"]
        );
        assert_eq!(schema.required, Some(Vec::new()));
    }
}
