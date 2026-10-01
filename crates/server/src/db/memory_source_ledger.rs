use std::str::FromStr;

use anyhow::Result;
use chrono::Utc;
use devo_protocol::SessionId;
use rusqlite::params;

use super::Database;

impl Database {
    pub fn record_memory_source_deletions(&self, sources: &[SessionId]) -> Result<()> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        let transaction = conn.transaction()?;
        for source in sources {
            transaction.execute(
                "INSERT OR IGNORE INTO pending_memory_source_deletions
                 (source_session_id, requested_at) VALUES (?1, ?2)",
                params![source.to_string(), Utc::now().to_rfc3339()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn pending_memory_source_deletions(&self) -> Result<Vec<SessionId>> {
        let conn = self.conn.lock().expect("database mutex poisoned");
        let mut statement = conn.prepare(
            "SELECT source_session_id FROM pending_memory_source_deletions
             ORDER BY requested_at, source_session_id",
        )?;
        let values = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        values
            .into_iter()
            .map(|value| SessionId::from_str(&value).map_err(Into::into))
            .collect()
    }

    pub fn has_pending_memory_source_deletions(&self) -> Result<bool> {
        let conn = self.conn.lock().expect("database mutex poisoned");
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM pending_memory_source_deletions)",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn record_external_context_sources(&self, sources: &[SessionId]) -> Result<()> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        let transaction = conn.transaction()?;
        for source in sources {
            transaction.execute(
                "INSERT OR IGNORE INTO memory_external_context_sources
                 (source_session_id, observed_at) VALUES (?1, ?2)",
                params![source.to_string(), Utc::now().to_rfc3339()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn has_external_context_source(&self, source: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("database mutex poisoned");
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM memory_external_context_sources WHERE source_session_id = ?1)",
            [source],
            |row| row.get(0),
        )?)
    }

    pub fn has_memory_source_intent(&self, source: &str) -> Result<bool> {
        let conn = self.conn.lock().expect("database mutex poisoned");
        Ok(memory_source_intent(&conn, source)?)
    }

    /// Holds the session-index lock through a memory commit, so an intent
    /// cannot be recorded between the final check and that commit.
    pub(crate) fn with_memory_source_intent<T>(
        &self,
        source: &str,
        action: impl FnOnce(bool) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let conn = self.conn.lock().expect("database mutex poisoned");
        action(memory_source_intent(&conn, source)?)
    }

    pub fn pending_external_context_sources(&self) -> Result<Vec<SessionId>> {
        let conn = self.conn.lock().expect("database mutex poisoned");
        let mut statement = conn.prepare(
            "SELECT source_session_id FROM memory_external_context_sources
             WHERE reconciled_at IS NULL ORDER BY observed_at, source_session_id",
        )?;
        let values = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        values
            .into_iter()
            .map(|value| SessionId::from_str(&value).map_err(Into::into))
            .collect()
    }

    pub fn finish_external_context_sources(&self, sources: &[SessionId]) -> Result<()> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        let transaction = conn.transaction()?;
        for source in sources {
            transaction.execute(
                "UPDATE memory_external_context_sources SET reconciled_at = ?1
                 WHERE source_session_id = ?2",
                params![Utc::now().to_rfc3339(), source.to_string()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn has_pending_external_context_sources(&self) -> Result<bool> {
        let conn = self.conn.lock().expect("database mutex poisoned");
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM memory_external_context_sources WHERE reconciled_at IS NULL)",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn finish_memory_source_deletions(&self, sources: &[SessionId]) -> Result<()> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        let transaction = conn.transaction()?;
        for source in sources {
            transaction.execute(
                "DELETE FROM pending_memory_source_deletions WHERE source_session_id = ?1",
                [source.to_string()],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }
}

fn memory_source_intent(conn: &rusqlite::Connection, source: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pending_memory_source_deletions
            WHERE source_session_id = ?1)
          OR EXISTS(SELECT 1 FROM memory_external_context_sources
            WHERE source_session_id = ?1)",
        [source],
        |row| row.get(0),
    )
}
