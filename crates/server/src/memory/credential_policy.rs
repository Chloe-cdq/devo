//! Finite credential-assignment policy shared by memory ingress and output.
//!
//! Only labels are normalized. Existing identifier prefixes are accepted before
//! the credential suffixes; ordinary key/token names with another suffix remain
//! safe. No body, value, or stored textual identity is rewritten.

use devo_safety::{InMemorySecretDetectorRegistry, SecretDetectorRegistry};

#[path = "credential_migration.rs"]
mod migration;
pub(in crate::memory) use migration::purge_unsafe_memory;

pub(in crate::memory) fn contains_secret(body: &str) -> bool {
    static ASSIGNMENT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let assignment = ASSIGNMENT.get_or_init(|| {
        regex::Regex::new(r#"(?i)\b([a-z0-9_]+(?:[\s_-]+[a-z0-9]+)*)["']?\s*[:=]\s*"#)
            .expect("valid memory credential assignment regex")
    });
    let assignment_match = assignment.captures_iter(body).any(|capture| {
        let label = capture[1]
            .bytes()
            .filter(u8::is_ascii_alphanumeric)
            .map(|byte| char::from(byte.to_ascii_lowercase()))
            .collect::<String>();
        if ![
            "apikey",
            "accesskey",
            "privatekey",
            "token",
            "secret",
            "password",
        ]
        .iter()
        .any(|alias| label.ends_with(alias))
        {
            return false;
        }
        // Do not consume any value bytes in the regex: a preceding ordinary
        // assignment must leave a nested credential label available to scan.
        let value = &body[capture.get(0).expect("complete assignment match").end()..];
        match value.chars().next() {
            Some(quote @ ('\"' | '\'')) => value[1..]
                .split(quote)
                .next()
                .is_some_and(|value| !value.is_empty()),
            Some(character) => !character.is_whitespace(),
            None => false,
        }
    });
    let lower_body = body.to_ascii_lowercase();
    let marker_match = [
        "sk-",
        "ghp_",
        "github_pat_",
        "xoxb-",
        "xoxp-",
        "bearer ",
        "-----begin ",
    ]
    .iter()
    .any(|marker| lower_body.contains(marker));
    assignment_match
        || marker_match
        || InMemorySecretDetectorRegistry::with_default_detectors()
            .all()
            .into_iter()
            .any(|detector| !detector.detect(body).is_empty())
}

#[cfg(test)]
mod tests {
    use super::super::contains_secret;
    use pretty_assertions::assert_eq;

    /// Trace: L2-DES-MEM-001 DD-6
    /// Verifies: label spelling never weakens a nonempty credential assignment.
    #[test]
    fn credential_assignment_label_variations_reject_nonempty_values() {
        for label in [
            "_API_KEY",
            "_password",
            "API key",
            "api_key",
            "api-key",
            "apiKey",
            "OpenAI API key",
            "OPENAI_API_KEY",
            "openaiApiKey",
            "AWS secret access key",
            "awsSecretAccessKey",
            "access key",
            "private key",
            "client secret",
            "service_token",
            "authToken",
            "db-password",
        ] {
            for separator in [":", "="] {
                for quote in ["", "\"", "'"] {
                    for value in ["ab", "abcdefghijklmnop"] {
                        let text =
                            format!("{quote}{label}{quote} {separator} {quote}{value}{quote}");
                        assert_eq!(contains_secret(&text), true, "credential label variant");
                    }
                }
            }
        }
    }

    /// Trace: L2-DES-MEM-001 DD-6
    /// Verifies: preceding noncredential assignments cannot consume a nested credential label.
    #[test]
    fn credential_nested_assignment_labels_are_detected() {
        for text in ["Credentials: API key: ab", "option = password=ab"] {
            assert_eq!(
                contains_secret(text),
                true,
                "nested or quoted credential assignment"
            );
        }
    }

    /// Trace: L2-DES-MEM-001 DD-6
    /// Verifies: a nonempty quoted credential value is rejected even when it is punctuation.
    #[test]
    fn credential_quoted_punctuation_value_is_detected() {
        assert_eq!(contains_secret("API key: \";\""), true);
    }

    /// Trace: L2-DES-MEM-001 DD-6
    /// Verifies: Whitespace within a credential label cannot weaken assignment recognition.
    #[test]
    fn credential_multiline_assignment_label_is_detected() {
        for text in ["API\nkey=ab", "API\u{2003}key=ab"] {
            assert_eq!(contains_secret(text), true);
        }
    }

    /// Trace: L2-DES-MEM-001 DD-6
    /// Verifies: whitespace after the assignment separator still introduces a credential value.
    #[test]
    fn credential_multiline_assignment_value_is_detected() {
        for text in ["API key:\nab", "password =\n ab", "API key:\u{2003}ab"] {
            assert_eq!(contains_secret(text), true);
        }
    }

    /// Trace: L2-DES-MEM-001 DD-6
    /// Verifies: a nonempty quoted value cannot be treated as empty by trimming it.
    #[test]
    fn credential_quoted_whitespace_value_is_detected() {
        for text in ["API key: \" \"", "password='\t'"] {
            assert_eq!(contains_secret(text), true);
        }
    }

    /// Trace: L2-DES-MEM-001 DD-6
    /// Verifies: punctuation values recognized by the original assignment policy remain rejected.
    #[test]
    fn credential_unquoted_punctuation_values_remain_detected() {
        for text in ["API key: ;", "password=}", "token=,"] {
            assert_eq!(contains_secret(text), true);
        }
    }

    /// Trace: L2-DES-MEM-001 DD-6
    /// Verifies: ordinary identifiers, filenames, and token counts remain eligible.
    #[test]
    fn credential_assignment_policy_preserves_safe_controls() {
        for text in [
            "token count: 5",
            "token_count=5",
            "max_tokens=1024",
            "api_key.rs: implementation",
            "api_key_file=credentials.json",
            "aws_secret_access_key.md",
            "monkey=abc",
            "tokenizer=abc",
            "API key:",
            "API key = \"\"",
            "password: ''",
            "FOO=1",
            "The password manager is local.",
            "Use apiKey to name the setting.",
        ] {
            assert_eq!(contains_secret(text), false, "noncredential control");
        }
    }
}

#[cfg(test)]
#[path = "credential_migration_tests.rs"]
mod migration_tests;
