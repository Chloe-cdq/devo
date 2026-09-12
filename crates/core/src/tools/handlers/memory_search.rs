use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use devo_protocol::native::rpc_memory::MemorySearchParams;
use serde_json::json;

use crate::contracts::ToolResultContent;
use crate::contracts::{ToolCallError, ToolContext, ToolProgressSender, ToolResult};
use crate::json_schema::JsonSchema;
use crate::tool_handler::ToolHandler;
use crate::tool_spec::ToolExecutionMode;
use crate::tool_spec::ToolOutputMode;
use crate::tool_spec::ToolPreparationFeedback;
use crate::tool_spec::ToolSpec;

use super::memory::memory_tool_invocation;

/// Built-in root-agent read action for selecting memory IDs before mutation.
pub struct MemorySearchHandler {
    spec: ToolSpec,
}

impl Default for MemorySearchHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl MemorySearchHandler {
    pub fn new() -> Self {
        Self {
            spec: memory_search_spec(),
        }
    }
}

pub fn memory_search_spec() -> ToolSpec {
    ToolSpec {
        name: "memory_search".to_string(),
        description: "Search bounded user or project memory summaries and stable entry IDs. For natural-language forget requests, show the results and ask the user to reply in a later turn with exactly 'Confirm forget memory entry <entry_id>' or '确认删除记忆条目 <entry_id>'; memory_forget cannot run in this same user turn.".to_string(),
        input_schema: JsonSchema::object(
            BTreeMap::from([
                (
                    "query".to_string(),
                    JsonSchema::string(Some("Text to search for in memory summaries.")),
                ),
                (
                    "scope".to_string(),
                    JsonSchema {
                        enum_values: Some(vec![json!("user"), json!("project")]),
                        ..JsonSchema::string(Some("Optional memory scope."))
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
                        ..JsonSchema::string(Some("Optional semantic kind."))
                    },
                ),
                (
                    "state".to_string(),
                    JsonSchema {
                        enum_values: Some(vec![
                            json!("active"),
                            json!("stale"),
                            json!("conflicted"),
                            json!("retired"),
                            json!("restored"),
                        ]),
                        ..JsonSchema::string(Some(
                            "Optional lifecycle state; active and restored entries by default.",
                        ))
                    },
                ),
            ]),
            Some(vec!["query".to_string()]),
            Some(/*additional_properties*/ false),
        ),
        output_mode: ToolOutputMode::StructuredJson,
        execution_mode: ToolExecutionMode::ReadOnly,
        capability_tags: vec![],
        supports_parallel: false,
        preparation_feedback: ToolPreparationFeedback::None,
        display_name: None,
        supports_cancellation: None,
        supports_streaming: None,
    }
}

#[async_trait]
impl ToolHandler for MemorySearchHandler {
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
        let params = parse_memory_search_input(&input)?;
        let invocation = memory_tool_invocation(&ctx, "memory_search")?;
        let coordinator = ctx.agent_coordinator.ok_or_else(|| {
            ToolCallError::NeedsConfiguration(
                "memory_search requires a server runtime coordinator".to_string(),
            )
        })?;
        let result = Arc::clone(&coordinator)
            .memory_search(invocation, params)
            .await?;
        let value = serde_json::to_value(result)
            .map_err(|error| ToolCallError::InternalError(error.to_string()))?;
        Ok(ToolResult::success(
            ToolResultContent::Json(value),
            "Memory search results",
        ))
    }
}

fn parse_memory_search_input(
    input: &serde_json::Value,
) -> Result<MemorySearchParams, ToolCallError> {
    let mut params =
        serde_json::from_value::<MemorySearchParams>(input.clone()).map_err(|error| {
            ToolCallError::InvalidInput(format!("invalid memory_search input: {error}"))
        })?;
    params.query = params.query.trim().to_string();
    if params.query.is_empty() {
        return Err(ToolCallError::InvalidInput(
            "memory_search requires 'query'".to_string(),
        ));
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::{memory_search_spec, parse_memory_search_input};
    use devo_protocol::native::rpc_memory::{MemoryKind, MemoryScope, MemoryState};

    /// Trace: L2-DES-MEM-001
    /// Verifies: memory search requires a non-empty query and parses optional filters.
    #[test]
    fn memory_search_parses_query_and_filters() {
        let parsed = parse_memory_search_input(&serde_json::json!({
            "query": " old timezone ",
            "scope": "user",
            "kind": "preference",
            "state": "active"
        }))
        .expect("valid memory search input");

        assert_eq!(
            parsed,
            devo_protocol::native::rpc_memory::MemorySearchParams {
                query: "old timezone".to_string(),
                scope: Some(MemoryScope::User),
                kind: Some(MemoryKind::Preference),
                state: Some(MemoryState::Active),
            }
        );
    }

    /// Trace: L2-DES-MEM-001
    /// Verifies: the memory search schema requires only the model's query and serializes pending-state writes.
    #[test]
    fn memory_search_schema_requires_query() {
        let spec = memory_search_spec();

        assert_eq!(spec.supports_parallel, false);
        assert_eq!(spec.input_schema.required, Some(vec!["query".to_string()]));
        assert_eq!(
            spec.input_schema
                .properties
                .expect("memory search properties")
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["kind", "query", "scope", "state"]
        );
    }
}
