const INTENT_PREFIXES: &[&str] = &[
    "i'd like you to remember that",
    "i'd like you to remember this",
    "i’d like you to remember that",
    "i’d like you to remember this",
    "i want you to remember that",
    "i want you to remember this",
    "can you remember that",
    "can you remember this",
    "could you remember that",
    "could you remember this",
    "would you remember that",
    "would you remember this",
    "i'd like you to remember",
    "i’d like you to remember",
    "i want you to remember",
    "please keep in mind that",
    "please remember that",
    "please remember this",
    "please keep in mind",
    "for future reference",
    "keep in mind that",
    "can you remember",
    "could you remember",
    "would you remember",
    "do not forget that",
    "do not forget this",
    "don't forget that",
    "don't forget this",
    "please note that",
    "please remember",
    "keep in mind",
    "do not forget",
    "don't forget",
    "memorize that",
    "memorize this",
    "store that",
    "store this",
    "save that",
    "save this",
    "remember that",
    "remember this",
    "please note",
    "note that",
    "remember",
    "note",
];

const ATTACHED_INTENT_PREFIXES: &[(&str, &str)] = &[
    ("请记住", "请记住"),
    ("请记一下", "请记一下"),
    ("请记下来", "请记下来"),
    ("帮我记住", "帮我记住"),
    ("记住这", "记住"),
    ("记住我", "记住"),
    ("记一下", "记一下"),
    ("记下来", "记下来"),
    ("请保存", "请保存"),
    ("帮我保存", "帮我保存"),
    ("保存一下", "保存一下"),
    ("保存这", "保存"),
    ("存一下", "存一下"),
    ("别忘了", "别忘了"),
    ("不要忘记", "不要忘记"),
];

pub(super) fn explicit_memory_key(body: &str) -> String {
    let original_normalized_key = normalize_tokens(body).join(" ");
    let mut body = body.trim();
    let mut removed_intent = false;
    loop {
        let removed_prefix = if let Some(prefix) = INTENT_PREFIXES.iter().find(|prefix| {
            let prefix = **prefix;
            body.get(..prefix.len())
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
                && body[prefix.len()..]
                    .chars()
                    .next()
                    .is_none_or(|character| !character.is_ascii_alphanumeric())
        }) {
            body = &body[prefix.len()..];
            true
        } else if let Some(frame) = ATTACHED_INTENT_PREFIXES
            .iter()
            .find(|frame| body.starts_with(frame.0))
        {
            body = &body[frame.1.len()..];
            true
        } else {
            false
        };
        if !removed_prefix {
            break;
        }
        removed_intent = true;
        body = body.trim_start();
        if let Some(separator) = body.chars().next().filter(|character| {
            matches!(
                character,
                ':' | ';' | ',' | '!' | '?' | '：' | '；' | '，' | '！' | '？'
            )
        }) {
            body = body[separator.len_utf8()..].trim_start();
        }
    }
    let normalized_tokens = normalize_tokens(body);
    let normalized_key = normalized_tokens.join(" ");
    let mut semantic_tokens = normalized_tokens;
    if semantic_tokens
        .last()
        .is_some_and(|token| token == "please")
    {
        semantic_tokens.pop();
    }

    let preference_prefix_length = if starts_with(&semantic_tokens, &["my", "preference", "is"])
        || starts_with(&semantic_tokens, &["i", "would", "prefer"])
    {
        Some(3)
    } else if starts_with(&semantic_tokens, &["i'd", "prefer"])
        || starts_with(&semantic_tokens, &["i’d", "prefer"])
    {
        Some(2)
    } else {
        None
    };
    if let Some(prefix_length) = preference_prefix_length {
        semantic_tokens.splice(..prefix_length, ["i".to_string(), "prefer".to_string()]);
    }

    let semantic_key = semantic_tokens.join(" ");
    if semantic_key.is_empty() {
        if removed_intent {
            original_normalized_key
        } else {
            normalized_key
        }
    } else {
        semantic_key
    }
}

fn normalize_tokens(body: &str) -> Vec<String> {
    body.split_whitespace()
        .filter_map(normalize_token)
        .collect()
}

fn normalize_token(token: &str) -> Option<String> {
    let unwrapped = token.trim_matches(is_structured_wrapper_punctuation);
    let trimmed = unwrapped.trim_matches(is_boundary_punctuation);
    if is_structured_token(unwrapped, trimmed) {
        let structured = unwrapped;
        let without_sentence_period = structured.trim_end_matches('.');
        let structured = if without_sentence_period.is_empty() {
            structured
        } else {
            without_sentence_period
        };
        (!structured.is_empty()).then(|| structured.to_string())
    } else if trimmed.is_empty() {
        None
    } else {
        Some(
            trimmed
                .chars()
                .flat_map(char::to_lowercase)
                .collect::<String>(),
        )
    }
}

fn is_structured_token(original: &str, trimmed: &str) -> bool {
    original.starts_with('.')
        || trimmed
            .chars()
            .any(|character| matches!(character, '/' | '\\' | ':' | '@' | '#' | '=' | '.'))
}

fn is_boundary_punctuation(character: char) -> bool {
    matches!(
        character,
        '.' | ','
            | '!'
            | '?'
            | ':'
            | ';'
            | '\''
            | '"'
            | '('
            | ')'
            | '['
            | ']'
            | '{'
            | '}'
            | '‘'
            | '’'
            | '“'
            | '”'
            | '。'
            | '，'
            | '！'
            | '？'
            | '：'
            | '；'
    )
}

fn is_structured_wrapper_punctuation(character: char) -> bool {
    matches!(
        character,
        ',' | '!'
            | '?'
            | ';'
            | '\''
            | '"'
            | '('
            | ')'
            | '['
            | ']'
            | '{'
            | '}'
            | '‘'
            | '’'
            | '“'
            | '”'
            | '。'
            | '，'
            | '！'
            | '？'
            | '；'
    )
}

fn starts_with(tokens: &[String], prefix: &[&str]) -> bool {
    tokens.len() >= prefix.len()
        && tokens
            .iter()
            .zip(prefix)
            .all(|(token, expected)| token == expected)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::{assert_eq, assert_ne};

    use super::explicit_memory_key;

    /// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-8
    /// Verifies: supported explicit-intent and preference frames share one deterministic key.
    #[test]
    fn explicit_intent_and_preference_frames_have_a_fixed_equivalence_table() {
        let cases = [
            (
                "Please remember that I prefer compact responses.",
                "i prefer compact responses",
            ),
            (
                "Please remember this: I prefer compact responses!",
                "i prefer compact responses",
            ),
            (
                "Please remember I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Remember that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Remember this: I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Remember I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Please keep in mind that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Please keep in mind I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Keep in mind that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Keep in mind I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "For future reference: I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Please note that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Please note I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Note that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Note I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "My preference is compact responses",
                "i prefer compact responses",
            ),
            (
                "I would prefer compact responses",
                "i prefer compact responses",
            ),
            ("I'd prefer compact responses", "i prefer compact responses"),
            ("I’d prefer compact responses", "i prefer compact responses"),
            (
                "Remember that please note I prefer compact responses please",
                "i prefer compact responses",
            ),
            (
                "Can you remember that I prefer compact responses?",
                "i prefer compact responses",
            ),
            (
                "Could you remember this: I prefer compact responses?",
                "i prefer compact responses",
            ),
            (
                "Would you remember I prefer compact responses?",
                "i prefer compact responses",
            ),
            (
                "I'd like you to remember that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "I'd like you to remember this: I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "I'd like you to remember I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "I’d like you to remember that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "I’d like you to remember this: I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "I’d like you to remember I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "I want you to remember that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "I want you to remember this: I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "I want you to remember I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Can you remember this: I prefer compact responses?",
                "i prefer compact responses",
            ),
            (
                "Can you remember I prefer compact responses?",
                "i prefer compact responses",
            ),
            (
                "Could you remember that I prefer compact responses?",
                "i prefer compact responses",
            ),
            (
                "Could you remember I prefer compact responses?",
                "i prefer compact responses",
            ),
            (
                "Would you remember that I prefer compact responses?",
                "i prefer compact responses",
            ),
            (
                "Would you remember this: I prefer compact responses?",
                "i prefer compact responses",
            ),
            (
                "Memorize that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Memorize this: I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Save this: I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Save that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Store that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Store this: I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Don't forget that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Don't forget this: I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Don't forget I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Do not forget that I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Do not forget this: I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Do not forget I prefer compact responses",
                "i prefer compact responses",
            ),
            ("请记住我喜欢简洁的回复。", "我喜欢简洁的回复"),
            ("帮我记住我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("请记一下我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("请记下来我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("记住我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("记住这个项目使用 Rust", "这个项目使用 rust"),
            ("记一下我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("记下来我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("请保存我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("帮我保存我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("保存这个偏好", "这个偏好"),
            ("保存一下我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("存一下我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("别忘了我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("不要忘记我喜欢简洁的回复", "我喜欢简洁的回复"),
            ("Remember!", "remember"),
            ("请记住！", "请记住"),
            ("请记住 Remember!", "请记住 remember"),
            (
                "Remember:I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Please remember?I prefer compact responses",
                "i prefer compact responses",
            ),
            (
                "Can you remember:https://example.com/Docs",
                "https://example.com/Docs",
            ),
            ("保存期限是 30 天", "保存期限是 30 天"),
            ("记住能力需要测试", "记住能力需要测试"),
            ("Rememberance matters", "rememberance matters"),
        ];

        for (input, expected) in cases {
            assert_eq!(explicit_memory_key(input), expected);
        }
    }

    /// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 DD-8
    /// Verifies: structured token identity survives explicit-memory key normalization.
    #[test]
    fn structured_tokens_remain_identity_bearing() {
        assert_ne!(
            explicit_memory_key("Use .env for configuration"),
            explicit_memory_key("Use env for configuration")
        );
        assert_ne!(
            explicit_memory_key("Use ../config"),
            explicit_memory_key("Use /config")
        );
        assert_ne!(explicit_memory_key("Use ."), explicit_memory_key("Use"));
        assert_ne!(explicit_memory_key("Use .."), explicit_memory_key("Use"));
        assert_ne!(
            explicit_memory_key("Use '.env' for configuration"),
            explicit_memory_key("Use env for configuration")
        );
        assert_ne!(
            explicit_memory_key("Use (.env) for configuration"),
            explicit_memory_key("Use env for configuration")
        );
        assert_ne!(explicit_memory_key("Use \".\""), explicit_memory_key("Use"));
        assert_ne!(explicit_memory_key("Use (..)"), explicit_memory_key("Use"));
        assert_eq!(
            explicit_memory_key("Use '.env' for configuration"),
            explicit_memory_key("Use .env for configuration")
        );
        assert_ne!(
            explicit_memory_key("Read config.toml"),
            explicit_memory_key("Read configtoml")
        );
        assert_ne!(
            explicit_memory_key("Use FOO=1"),
            explicit_memory_key("Use foo=1")
        );
        assert_ne!(
            explicit_memory_key("Remember https://example.com/Docs"),
            explicit_memory_key("Remember https://example.com/docs")
        );
        assert_ne!(
            explicit_memory_key("Use /Config"),
            explicit_memory_key("Use /config")
        );
        assert_eq!(
            explicit_memory_key("Remember “https://example.com/Docs”"),
            explicit_memory_key("Remember https://example.com/Docs")
        );
        assert_eq!(
            explicit_memory_key("Use config.toml."),
            explicit_memory_key("Use config.toml")
        );
    }
}
