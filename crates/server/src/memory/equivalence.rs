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
    let token = token.trim_matches(is_boundary_punctuation);
    if token.is_empty() {
        return None;
    }
    Some(token.chars().flat_map(char::to_lowercase).collect())
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

fn starts_with(tokens: &[String], prefix: &[&str]) -> bool {
    tokens.len() >= prefix.len()
        && tokens
            .iter()
            .zip(prefix)
            .all(|(token, expected)| token == expected)
}
