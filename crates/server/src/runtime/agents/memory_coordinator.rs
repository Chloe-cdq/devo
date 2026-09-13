use devo_core::tools::MemoryToolInvocation;

use super::*;

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
    let user_text = runtime
        .current_user_item_text(
            invocation.session_id,
            invocation.turn_id,
            &invocation.user_item_id,
        )
        .await
        .map_err(|error| ToolCallError::InvalidInput(error.to_string()))?;
    if !has_explicit_memory_intent(&user_text) {
        return Err(ToolCallError::InvalidInput(
            "memory_remember requires explicit intent in the current user message".to_string(),
        ));
    }
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
        | crate::memory::MemoryCommandResult::Forget(_)
        | crate::memory::MemoryCommandResult::List(_) => Err(ToolCallError::InternalError(
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
    let authorized = runtime.memory_forget_coordinator.authorize_agent(
        &invocation,
        &user_text,
        &entry_id,
        params.scope,
    )?;
    let result = runtime
        .deps
        .memory_forget_executor
        .execute(
            &context.memory,
            crate::memory::MemoryForgetRequest {
                selector: crate::memory::MemoryForgetSelector::EntryId(entry_id),
                scope: authorized.scope,
                source: context.source,
            },
        )
        .await
        .map_err(memory_tool_error)?;
    match result {
        crate::memory::MemoryCommandResult::Forget(result) => {
            authorized.reservation.commit(result.forgotten.as_ref())?;
            Ok(result)
        }
        crate::memory::MemoryCommandResult::Status(_)
        | crate::memory::MemoryCommandResult::Remember(_)
        | crate::memory::MemoryCommandResult::List(_) => Err(ToolCallError::InternalError(
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
    let states = params.state.map_or_else(
        || {
            vec![
                devo_protocol::native::rpc_memory::MemoryState::Active,
                devo_protocol::native::rpc_memory::MemoryState::Restored,
            ]
        },
        |state| vec![state],
    );
    let mut entries = Vec::new();
    for state in states {
        let result = memory
            .execute_command(crate::memory::MemoryCommand::List(
                crate::memory::ListMemoryRequest {
                    scope: Some(scope),
                    kind: params.kind,
                    state: Some(state),
                    origin: None,
                    text: Some(params.query.clone()),
                    cursor: None,
                    limit: Some(20),
                    workspace_root: workspace_root.clone(),
                },
            ))
            .await
            .map_err(memory_tool_error)?;
        match result {
            crate::memory::MemoryCommandResult::List(page) => entries.extend(page.data),
            crate::memory::MemoryCommandResult::Status(_)
            | crate::memory::MemoryCommandResult::Remember(_)
            | crate::memory::MemoryCommandResult::Forget(_) => {
                return Err(ToolCallError::InternalError(
                    "memory_search returned an unexpected result".to_string(),
                ));
            }
        }
    }
    entries.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.entry_id.cmp(&right.entry_id))
    });
    entries.truncate(20);
    let result = devo_protocol::native::page::Page {
        data: entries
            .into_iter()
            .map(
                |entry| devo_protocol::native::rpc_memory::MemorySearchEntry {
                    entry_id: entry.entry_id,
                    scope: entry.scope,
                    kind: entry.kind,
                    state: entry.state,
                    summary: {
                        const MAX_SUMMARY_CHARS: usize = 240;
                        let mut summary = entry
                            .body
                            .chars()
                            .take(MAX_SUMMARY_CHARS)
                            .collect::<String>();
                        if entry.body.chars().count() > MAX_SUMMARY_CHARS {
                            summary.push('…');
                        }
                        summary
                    },
                },
            )
            .collect::<Vec<_>>(),
        next_cursor: None,
    };
    runtime
        .memory_forget_coordinator
        .record_search(&invocation, &result.data)?;
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
        crate::memory::MemoryError::Directory(_)
        | crate::memory::MemoryError::Database(_)
        | crate::memory::MemoryError::LockPoisoned
        | crate::memory::MemoryError::InvalidCount(_)
        | crate::memory::MemoryError::InvalidTimestamp(_)
        | crate::memory::MemoryError::ProjectIdentity(_)
        | crate::memory::MemoryError::InvalidStoredValue(_) => {
            ToolCallError::InternalError("memory operation is unavailable".to_string())
        }
    }
}

fn has_explicit_memory_intent(text: &str) -> bool {
    let text = text.trim_start().to_ascii_lowercase();
    if text.starts_with("don't remember")
        || text.starts_with("do not remember")
        || text.starts_with("i remember")
        || text.starts_with("we remember")
        || text.starts_with("不要保存")
        || text.starts_with("不要记")
        || text.starts_with("请勿记")
    {
        return false;
    }
    [
        "please remember",
        "can you remember",
        "could you remember",
        "would you remember",
        "i want you to remember",
        "i'd like you to remember",
        "remember:",
        "remember this",
        "remember that",
        "remember my",
        "remember i",
        "memorize this",
        "memorize that",
        "keep in mind",
        "save this",
        "save that",
        "store this",
        "store that",
        "don't forget",
        "do not forget",
        "请记住",
        "请记一下",
        "请记下来",
        "帮我记住",
        "记住这",
        "记住我",
        "记一下",
        "记下来",
        "请保存",
        "帮我保存",
        "保存这",
        "保存一下",
        "存一下",
        "别忘了",
        "不要忘记",
    ]
    .iter()
    .any(|phrase| {
        text.strip_prefix(phrase).is_some_and(|remainder| {
            let boundary_is_valid = !phrase
                .chars()
                .last()
                .is_some_and(|character| character.is_ascii_alphanumeric())
                || remainder
                    .chars()
                    .next()
                    .is_none_or(|character| !character.is_ascii_alphanumeric());
            boundary_is_valid && remainder.chars().any(char::is_alphanumeric)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::has_explicit_memory_intent;

    /// Trace: L2-DES-MEM-001
    /// Verifies: supported explicit memory requests are recognized in English and Chinese.
    #[test]
    fn explicit_memory_intent_accepts_english_and_chinese_requests() {
        assert!(has_explicit_memory_intent(
            "Please remember that I prefer tabs"
        ));
        assert!(has_explicit_memory_intent("Remember: I prefer tabs"));
        assert!(has_explicit_memory_intent("请记住我喜欢深色模式"));
        assert!(has_explicit_memory_intent("Can you remember my timezone?"));
        assert!(has_explicit_memory_intent("别忘了我不喝咖啡"));
        assert!(!has_explicit_memory_intent("I prefer tabs"));
        assert!(!has_explicit_memory_intent("Please remember"));
        assert!(!has_explicit_memory_intent("Please rememberable tabs"));
    }

    /// Trace: L2-DES-MEM-001
    /// Verifies: negated or descriptive memory phrases are rejected.
    #[test]
    fn explicit_memory_intent_rejects_negation_and_description() {
        assert!(!has_explicit_memory_intent("Don't remember my birthday"));
        assert!(!has_explicit_memory_intent("Do not save this"));
        assert!(!has_explicit_memory_intent("不要保存我的生日"));
        assert!(!has_explicit_memory_intent(
            "Explain 'please remember'; do not save anything"
        ));
        assert!(!has_explicit_memory_intent("请勿记住这件事"));
        assert!(!has_explicit_memory_intent("I remember my birthday"));
        assert!(!has_explicit_memory_intent("我保存过这个"));
    }
}
