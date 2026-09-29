use thiserror::Error;

#[derive(Debug, Error)]
pub enum AgentError {
    #[error("model provider error: {0}")]
    Provider(#[from] anyhow::Error),

    #[error("max turns ({0}) exceeded")]
    MaxTurnsExceeded(usize),

    #[error("context too long after compaction")]
    ContextTooLong,

    #[error("session aborted by user")]
    Aborted,
}

impl AgentError {
    /// Private details for Native user-facing failures; never diagnostic logs.
    pub fn user_message(&self) -> String {
        match self {
            Self::Provider(error) => format!(
                "model provider error: {}",
                devo_provider::diagnostic::user_message_for_error(error)
            ),
            Self::MaxTurnsExceeded(_) | Self::ContextTooLong | Self::Aborted => self.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_messages() {
        let err = AgentError::MaxTurnsExceeded(10);
        assert_eq!(err.to_string(), "max turns (10) exceeded");

        let err = AgentError::ContextTooLong;
        assert_eq!(err.to_string(), "context too long after compaction");

        let err = AgentError::Aborted;
        assert_eq!(err.to_string(), "session aborted by user");
    }

    #[test]
    fn provider_error_from_anyhow() {
        let anyhow_err = anyhow::anyhow!("connection refused");
        let err: AgentError = anyhow_err.into();
        assert!(err.to_string().contains("connection refused"));
    }
}
