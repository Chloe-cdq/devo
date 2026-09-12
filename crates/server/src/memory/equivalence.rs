const INTENT_PREFIXES: &[&[&str]] = &[
    &["please", "keep", "in", "mind", "that"],
    &["please", "remember", "that"],
    &["please", "remember", "this"],
    &["please", "keep", "in", "mind"],
    &["for", "future", "reference"],
    &["keep", "in", "mind", "that"],
    &["please", "note", "that"],
    &["please", "remember"],
    &["keep", "in", "mind"],
    &["remember", "that"],
    &["remember", "this"],
    &["please", "note"],
    &["note", "that"],
    &["remember"],
    &["note"],
];

pub(super) fn explicit_memory_key(body: &str) -> String {
    let normalized_tokens = body
        .split_whitespace()
        .filter_map(normalize_token)
        .collect::<Vec<_>>();
    let normalized_key = normalized_tokens.join(" ");
    let mut semantic_tokens = normalized_tokens;

    while let Some(prefix) = INTENT_PREFIXES
        .iter()
        .find(|prefix| starts_with(&semantic_tokens, prefix))
    {
        semantic_tokens.drain(..prefix.len());
    }
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
        normalized_key
    } else {
        semantic_key
    }
}

fn normalize_token(token: &str) -> Option<String> {
    let trimmed = token.trim_matches(is_boundary_punctuation);
    if is_structured_token(token, trimmed) {
        let structured = token.trim_matches(is_structured_wrapper_punctuation);
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
            ("Remember!", "remember"),
        ];

        for (input, expected) in cases {
            assert_eq!(explicit_memory_key(input), expected);
        }
    }

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
