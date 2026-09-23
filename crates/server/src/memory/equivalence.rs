pub(super) fn explicit_memory_key(body: &str) -> String {
    let collapsed = body.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut claim = collapsed.as_str();

    loop {
        let mut unwrapped = None;
        for (opening, closing) in [
            ('"', '"'),
            ('\'', '\''),
            ('“', '”'),
            ('‘', '’'),
            ('(', ')'),
            ('[', ']'),
            ('{', '}'),
        ] {
            if claim.len() >= opening.len_utf8() + closing.len_utf8()
                && claim.starts_with(opening)
                && claim.ends_with(closing)
            {
                let inner = claim[opening.len_utf8()..claim.len() - closing.len_utf8()].trim();
                if !inner.is_empty() {
                    unwrapped = Some(inner);
                    break;
                }
            }
        }
        if let Some(inner) = unwrapped {
            claim = inner;
            continue;
        }

        if let Some(last) = claim.chars().last()
            && matches!(last, '.' | '!' | '。' | '！')
        {
            let without_delimiter = &claim[..claim.len() - last.len_utf8()];
            if without_delimiter.chars().last().is_some_and(|character| {
                character.is_alphabetic()
                    || matches!(character, '"' | '\'' | '”' | '’' | ')' | ']' | '}')
            }) {
                claim = without_delimiter;
                continue;
            }
        }
        break;
    }

    let characters = claim.chars().collect::<Vec<_>>();
    let ambiguous_case = claim.split_whitespace().enumerate().any(|(index, token)| {
        let token = token.trim_end_matches(',');
        if index == 0 {
            !matches!(token, "I" | "My" | "The") && token.chars().any(char::is_uppercase)
        } else {
            token.chars().any(char::is_uppercase)
        }
    });
    let plain_prose = characters.iter().enumerate().all(|(index, character)| {
        character.is_alphabetic()
            || character.is_whitespace()
            || (*character == ','
                && index > 0
                && characters[index - 1].is_alphabetic()
                && characters
                    .get(index + 1)
                    .is_some_and(|next| next.is_whitespace()))
    });
    if plain_prose && claim.split_whitespace().count() > 1 && !ambiguous_case {
        claim.to_lowercase()
    } else {
        collapsed
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::{assert_eq, assert_ne};

    use super::explicit_memory_key;

    /// Trace: L2-DES-MEM-001 DD-8
    /// Verifies: an authorized claim keeps its wording while prose formatting normalizes.
    #[test]
    fn explicit_key_normalizes_only_plain_prose_formatting() {
        assert_eq!(
            explicit_memory_key("  I   prefer compact responses!  "),
            "i prefer compact responses"
        );
        assert_eq!(
            explicit_memory_key("Remember that I prefer compact responses"),
            "Remember that I prefer compact responses"
        );
        assert_eq!(
            explicit_memory_key("\"I prefer compact responses\"."),
            "i prefer compact responses"
        );
        assert_eq!(
            explicit_memory_key("(I prefer compact responses)."),
            "i prefer compact responses"
        );
    }

    /// Trace: L2-DES-MEM-001 DD-8
    /// Verifies: an ordinary article-led sentence folds its initial capital.
    #[test]
    fn explicit_key_case_folds_plain_prose_article() {
        assert_eq!(explicit_memory_key("The sky is blue."), "the sky is blue");
    }

    /// Trace: L2-DES-MEM-001 DD-8
    /// Verifies: an ordinary possessive-led sentence folds its initial capital.
    #[test]
    fn explicit_key_case_folds_plain_prose_possessive() {
        assert_eq!(
            explicit_memory_key("My preference is compact responses"),
            "my preference is compact responses"
        );
    }

    /// Trace: L2-DES-MEM-001 DD-8
    /// Verifies: an internal prose comma stays present while surrounding words case-fold.
    #[test]
    fn explicit_key_case_folds_unambiguous_prose_with_internal_comma() {
        assert_eq!(
            explicit_memory_key("I prefer tea, not coffee."),
            "i prefer tea, not coffee"
        );
        assert_ne!(
            explicit_memory_key("FOO, BAR"),
            explicit_memory_key("foo, bar")
        );
    }

    /// Trace: L2-DES-MEM-001 DD-8
    /// Verifies: a question remains distinct from the corresponding assertion.
    #[test]
    fn explicit_key_preserves_question_marks() {
        assert_ne!(
            explicit_memory_key("I prefer tea?"),
            explicit_memory_key("I prefer tea")
        );
        assert_ne!(
            explicit_memory_key("I prefer tea？"),
            explicit_memory_key("I prefer tea")
        );
    }

    /// Trace: L2-DES-MEM-001 DD-8
    /// Verifies: a bare or embedded uppercase identifier retains its case.
    #[test]
    fn explicit_key_preserves_ambiguous_uppercase_identifiers() {
        assert_ne!(explicit_memory_key("FOO"), explicit_memory_key("foo"));
        assert_ne!(
            explicit_memory_key("Use FOO"),
            explicit_memory_key("use foo")
        );
        assert_ne!(
            explicit_memory_key("Use Foo"),
            explicit_memory_key("use foo")
        );
        assert_ne!(
            explicit_memory_key("The project uses Rust"),
            explicit_memory_key("the project uses rust")
        );
        assert_ne!(
            explicit_memory_key("Foo is enabled"),
            explicit_memory_key("foo is enabled")
        );
        assert_ne!(
            explicit_memory_key("X is enabled"),
            explicit_memory_key("x is enabled")
        );
    }

    /// Trace: L2-DES-MEM-001 DD-8
    /// Verifies: ambiguous structured punctuation and identifier case remain identity-bearing.
    #[test]
    fn explicit_key_keeps_structured_claims_opaque() {
        let cases = [
            ("Use API_KEY", "Use api_key"),
            ("Use FOO=1", "Use foo=1"),
            ("Use /Config", "Use /config"),
            ("Use config.toml:", "Use config.toml"),
            ("Use config.toml.", "Use config.toml"),
            ("Use C:", "Use C"),
        ];
        for (left, right) in cases {
            assert_ne!(explicit_memory_key(left), explicit_memory_key(right));
        }
        assert_eq!(explicit_memory_key("  Use  API_KEY  "), "Use API_KEY");
        assert_eq!(explicit_memory_key("Use config.toml:"), "Use config.toml:");
    }

    /// Trace: L2-DES-MEM-001 DD-8
    /// Verifies: structured punctuation remains identity-bearing under conservative keys.
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
        assert_ne!(
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
        assert_ne!(
            explicit_memory_key("Remember “https://example.com/Docs”"),
            explicit_memory_key("Remember https://example.com/Docs")
        );
        assert_ne!(
            explicit_memory_key("Use config.toml."),
            explicit_memory_key("Use config.toml")
        );
    }
}
