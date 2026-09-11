use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExplicitMemoryIntent {
    Remember,
    Forget,
}

impl ExplicitMemoryIntent {
    fn matches(self, text: &str) -> bool {
        match self {
            Self::Remember => has_explicit_memory_intent(text),
            Self::Forget => has_explicit_memory_forget_intent(text),
        }
    }
}

struct MemoryMutationContext {
    memory: Arc<crate::memory::MemoryRuntime>,
    source: crate::memory::MemorySourceContext,
}

impl MemoryMutationContext {
    async fn authorize(
        runtime: Arc<ServerRuntime>,
        session_id: String,
        turn_id: String,
        source_user_item_id: Option<devo_protocol::native::ids::ItemId>,
        intent: ExplicitMemoryIntent,
    ) -> Result<Self, ToolCallError> {
        let session_id = SessionId::try_from(session_id.as_str())
            .map_err(|error| ToolCallError::InvalidInput(error.to_string()))?;
        let turn_id = TurnId::try_from(turn_id.as_str())
            .map_err(|error| ToolCallError::InvalidInput(error.to_string()))?;
        let operation = match intent {
            ExplicitMemoryIntent::Remember => "memory_remember",
            ExplicitMemoryIntent::Forget => "memory_forget",
        };
        let source_item_id = source_user_item_id.ok_or_else(|| {
            ToolCallError::InvalidInput(format!(
                "{operation} requires the current user message context"
            ))
        })?;
        let source_item_id_string = source_item_id.to_string();
        if !has_explicit_current_user_memory_intent(
            &runtime,
            session_id,
            turn_id,
            &source_item_id_string,
            intent,
        )
        .await
        {
            return Err(ToolCallError::InvalidInput(format!(
                "{operation} requires explicit intent in the current user message"
            )));
        }
        let memory = runtime.memory.clone().ok_or_else(|| {
            ToolCallError::NeedsConfiguration("memory runtime is unavailable".to_string())
        })?;
        let summary = runtime
            .session_summary_snapshot(session_id)
            .await
            .ok_or_else(|| ToolCallError::InvalidInput("session not found".to_string()))?;
        let source = crate::memory::MemorySourceContext {
            user_item_id: Some(source_item_id),
            session_id,
            turn_id: Some(turn_id),
            workspace_root: summary.cwd,
        };
        Ok(Self { memory, source })
    }
}

async fn has_explicit_current_user_memory_intent(
    runtime: &ServerRuntime,
    session_id: SessionId,
    turn_id: TurnId,
    source_item_id: &str,
    intent: ExplicitMemoryIntent,
) -> bool {
    if let Some(stream) = runtime.active_stream_state(session_id).await {
        let stream = stream.lock().await;
        stream.turn_inline.as_ref().is_some_and(|inline| {
            inline.turn_id == turn_id
                && inline.persisted_turn_items.iter().any(|item| {
                    item.turn_id == turn_id
                        && item.item_id.to_string() == source_item_id
                        && matches!(
                            &item.turn_item,
                            devo_core::TurnItem::UserMessage(text)
                                if intent.matches(&text.text)
                        )
                })
        })
    } else {
        false
    }
}

pub(super) async fn remember(
    runtime: Arc<ServerRuntime>,
    session_id: String,
    turn_id: String,
    params: devo_protocol::native::rpc_memory::MemoryRememberParams,
) -> Result<devo_protocol::native::rpc_memory::MemoryEntry, ToolCallError> {
    let context = MemoryMutationContext::authorize(
        runtime,
        session_id,
        turn_id,
        params.source_user_item_id.clone(),
        ExplicitMemoryIntent::Remember,
    )
    .await?;
    let result = context
        .memory
        .execute_command(crate::memory::MemoryCommand::Remember(
            crate::memory::MemoryRememberRequest {
                text: params.text,
                scope: params.scope,
                kind: params.kind,
                source: context.source.clone(),
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
    session_id: String,
    turn_id: String,
    params: devo_protocol::native::rpc_memory::MemoryForgetParams,
) -> Result<devo_protocol::native::rpc_memory::MemoryForgetResult, ToolCallError> {
    let context = MemoryMutationContext::authorize(
        runtime,
        session_id,
        turn_id,
        params.source_user_item_id.clone(),
        ExplicitMemoryIntent::Forget,
    )
    .await?;
    let result = context
        .memory
        .execute_command(crate::memory::MemoryCommand::Forget(
            crate::memory::MemoryForgetRequest {
                selector: crate::memory::MemoryForgetSelector::from_params(&params)
                    .map_err(|message| ToolCallError::InvalidInput(message.to_string()))?,
                scope: params.scope,
                source: context.source.clone(),
            },
        ))
        .await
        .map_err(memory_tool_error)?;
    match result {
        crate::memory::MemoryCommandResult::Forget(result) => Ok(result),
        crate::memory::MemoryCommandResult::Status(_)
        | crate::memory::MemoryCommandResult::Remember(_)
        | crate::memory::MemoryCommandResult::List(_) => Err(ToolCallError::InternalError(
            "memory_forget returned an unexpected result".to_string(),
        )),
    }
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
    .any(|phrase| memory_command_has_payload(&text, phrase))
}

fn has_explicit_memory_forget_intent(text: &str) -> bool {
    let text = text.trim_start().to_ascii_lowercase();
    if text.starts_with("don't forget") || text.starts_with("do not forget") {
        return false;
    }
    [
        "please forget",
        "can you forget",
        "could you forget",
        "would you forget",
        "i want you to forget",
        "i'd like you to forget",
        "forget this",
        "forget that",
        "forget my",
        "forget about",
        "forget memory entry",
        "remove this from memory",
        "remove that from memory",
        "delete this memory",
        "delete that memory",
        "please remove from memory",
        "please delete from memory",
        "请忘记",
        "请删除",
        "忘记这",
        "忘记那",
        "删除这条记忆",
        "删除那条记忆",
        "删除记忆条目",
    ]
    .iter()
    .any(|phrase| memory_command_has_forget_payload(&text, phrase))
}

fn memory_command_has_payload(text: &str, phrase: &str) -> bool {
    memory_command_payload(text, phrase)
        .is_some_and(|remainder| remainder.chars().any(char::is_alphanumeric))
}

fn memory_command_has_forget_payload(text: &str, phrase: &str) -> bool {
    let Some(remainder) = memory_command_payload(text, phrase) else {
        return false;
    };
    let remainder = remainder.trim_start_matches(|character: char| {
        character.is_whitespace()
            || character.is_ascii_punctuation()
            || matches!(character, '：' | '，' | '。' | '；' | '、')
    });
    let has_payload = remainder.chars().any(char::is_alphanumeric);
    let phrase_names_memory = phrase.contains("memory") || phrase.contains("记忆");
    if !has_payload {
        return phrase_names_memory;
    }
    let suffix_is_boundary = |suffix: &str| {
        suffix
            .chars()
            .next()
            .is_none_or(|character| !character.is_alphanumeric())
    };
    let suffix_is_delimited = |suffix: &str| {
        suffix.is_empty()
            || suffix.chars().next().is_some_and(|character| {
                character.is_ascii_punctuation()
                    || matches!(character, '：' | '，' | '。' | '；' | '、')
            })
    };
    let starts_with_subject = |subject: &str| {
        remainder.strip_prefix(subject).is_some_and(|suffix| {
            if subject == "memory entry" || subject == "记忆条目" {
                suffix_is_boundary(suffix)
            } else {
                suffix_is_delimited(suffix)
            }
        })
    };
    let has_memory_subject = remainder == "memory"
        || remainder == "记忆"
        || [
            "this memory",
            "that memory",
            "the memory",
            "my memory",
            "memory entry",
            "这条记忆",
            "那条记忆",
            "我的记忆",
            "记忆条目",
        ]
        .iter()
        .any(|subject| starts_with_subject(subject));
    if phrase == "请删除" {
        return has_memory_subject;
    }
    let has_english_memory_statement = [
        "i prefer ",
        "i like ",
        "i love ",
        "i dislike ",
        "i hate ",
        "i use ",
        "i don't use ",
        "i do not use ",
        "i am ",
        "i'm ",
        "i have ",
        "i live ",
        "i work ",
        "that i prefer ",
        "that i like ",
        "that i love ",
        "that i dislike ",
        "that i hate ",
        "that i use ",
        "that i don't use ",
        "that i do not use ",
        "that i am ",
        "that i'm ",
        "that i have ",
        "that i live ",
        "that i work ",
    ]
    .iter()
    .any(|prefix| remainder.starts_with(prefix));
    let has_chinese_memory_statement = [
        "我喜欢",
        "我偏好",
        "我不喜欢",
        "我不喝",
        "我不吃",
        "我不想",
        "我不使用",
        "我不会",
        "我是",
        "我有",
        "我在",
        "我会",
        "我要",
        "我用",
        "我需要",
        "我习惯",
        "我通常",
        "我住",
        "我来自",
        "我叫",
    ]
    .iter()
    .any(|prefix| remainder.starts_with(prefix) && !remainder.contains('的'))
        || [
            "我喜欢的颜色",
            "我喜欢的主题",
            "我喜欢的模式",
            "我喜欢的语言",
            "我偏好的颜色",
            "我偏好的主题",
            "我偏好的模式",
            "我偏好的语言",
        ]
        .iter()
        .any(|prefix| remainder.starts_with(prefix));
    let personal_memory_subjects = [
        "timezone",
        "birthday",
        "name",
        "preference",
        "preferences",
        "favorite",
        "favourite",
        "language",
        "locale",
        "location",
        "pronouns",
        "theme",
    ];
    let english_personal_subject = remainder.strip_prefix("that ").unwrap_or(remainder);
    let has_personal_memory_subject = english_personal_subject.starts_with("my ")
        && personal_memory_subjects
            .iter()
            .any(|subject| english_personal_subject.contains(subject))
        || phrase == "forget my"
            && personal_memory_subjects.iter().any(|subject| {
                remainder.strip_prefix(subject).is_some_and(|suffix| {
                    suffix
                        .chars()
                        .next()
                        .is_none_or(|character| !character.is_alphanumeric())
                })
            })
        || [
            "我的时区",
            "我的生日",
            "我的名字",
            "我的偏好",
            "我的语言",
            "我的位置",
        ]
        .iter()
        .any(|subject| remainder.starts_with(subject));
    let is_task_lead = phrase.ends_with("forget about")
        || ["about", "all about", "everything about"]
            .iter()
            .any(|prefix| {
                remainder.strip_prefix(prefix).is_some_and(|suffix| {
                    suffix
                        .chars()
                        .next()
                        .is_none_or(|character| !character.is_ascii_alphanumeric())
                })
            });
    let contains_subject = |subject: &str, suffix_is_valid: &dyn Fn(&str) -> bool| {
        remainder.match_indices(subject).any(|(index, _)| {
            let prefix = &remainder[..index];
            let suffix = &remainder[index + subject.len()..];
            let prefix_is_boundary = prefix
                .chars()
                .next_back()
                .is_none_or(|character| !character.is_alphanumeric());
            prefix_is_boundary && suffix_is_valid(suffix)
        })
    };
    let names_specific_memory = [
        "this memory",
        "that memory",
        "the memory",
        "my memory",
        "这条记忆",
        "那条记忆",
        "我的记忆",
    ]
    .iter()
    .any(|subject| contains_subject(subject, &suffix_is_delimited))
        || ["memory entry", "记忆条目"]
            .iter()
            .any(|subject| contains_subject(subject, &suffix_is_boundary));

    has_payload
        && (phrase_names_memory && (!is_task_lead || names_specific_memory)
            || has_english_memory_statement
            || has_chinese_memory_statement
            || has_personal_memory_subject
            || names_specific_memory
            || (has_memory_subject && (!is_task_lead || names_specific_memory)))
}

fn memory_command_payload<'a>(text: &'a str, phrase: &str) -> Option<&'a str> {
    let remainder = text.strip_prefix(phrase)?;
    let boundary_is_valid = !phrase
        .chars()
        .last()
        .is_some_and(|character| character.is_ascii_alphanumeric())
        || remainder
            .chars()
            .next()
            .is_none_or(|character| !character.is_ascii_alphanumeric());
    boundary_is_valid.then_some(remainder)
}

#[cfg(test)]
mod tests {
    use super::{has_explicit_memory_forget_intent, has_explicit_memory_intent};

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

    /// Trace: L2-DES-MEM-001 DD-12
    /// Verifies: forget authorization is distinct from a remember request and its negation.
    #[test]
    fn explicit_memory_forget_intent_accepts_deletion_requests_only() {
        assert!(has_explicit_memory_forget_intent(
            "Please forget my old timezone"
        ));
        assert!(has_explicit_memory_forget_intent(
            "Forget that I prefer tabs"
        ));
        assert!(has_explicit_memory_forget_intent("Forget my timezone"));
        assert!(has_explicit_memory_forget_intent(
            "Please forget that my favorite color is blue"
        ));
        assert!(has_explicit_memory_forget_intent("请忘记我喜欢深色模式"));
        assert!(has_explicit_memory_forget_intent(
            "请忘记我喜欢的颜色是蓝色"
        ));
        assert!(has_explicit_memory_forget_intent("请删除这条记忆"));
        assert!(has_explicit_memory_forget_intent(
            "Remove this from memory: my old timezone"
        ));
        assert!(has_explicit_memory_forget_intent(
            "Delete this memory: old timezone"
        ));
        assert!(!has_explicit_memory_forget_intent(
            "Don't forget my timezone"
        ));
        assert!(!has_explicit_memory_forget_intent(
            "Please remember my timezone"
        ));
        assert!(!has_explicit_memory_forget_intent("Please forget"));
        assert!(!has_explicit_memory_forget_intent(
            "Forget about adding tests; implement B"
        ));
        assert!(!has_explicit_memory_forget_intent(
            "Forget about memory safety; implement UI"
        ));
        assert!(!has_explicit_memory_forget_intent(
            "Forget about the memory safety issue; implement UI"
        ));
        assert!(!has_explicit_memory_forget_intent(
            "Please forget that I asked for tests; implement docs"
        ));
        assert!(!has_explicit_memory_forget_intent(
            "Please forget memory safety; implement UI"
        ));
        assert!(has_explicit_memory_forget_intent(
            "Forget memory entry mem_123"
        ));
        assert!(has_explicit_memory_forget_intent("删除记忆条目 mem_123"));
        for text in [
            "Please forget about adding tests; implement B",
            "Can you forget about adding tests; implement B",
            "Could you forget about adding tests; implement B",
            "Would you forget about adding tests; implement B",
            "I want you to forget about adding tests; implement B",
            "I'd like you to forget about adding tests; implement B",
            "Please forget all about adding tests; implement B",
            "Please forget everything about adding tests; implement B",
            "请删除这个文件",
            "Please forget my changes",
            "Please forget my settings",
            "请删除我的文件",
            "请忘记我的设置",
            "请删除我不需要的文件",
            "请删除我喜欢的文件",
            "请忘记我喜欢的文件",
        ] {
            assert!(!has_explicit_memory_forget_intent(text), "{text}");
        }
        assert!(has_explicit_memory_forget_intent(
            "Forget about this memory"
        ));
        assert!(has_explicit_memory_forget_intent(
            "Please forget about this memory"
        ));
        assert!(has_explicit_memory_forget_intent(
            "Please forget all about this memory"
        ));
    }
}
