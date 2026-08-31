//! Server-owned General Persistent Memory runtime.
//!
//! This module deliberately exposes one high-level command surface. SQLite
//! tables are an implementation detail and are never returned to callers.

use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use devo_core::MemoryConfig;
use devo_protocol::native::rpc_memory::MemoryStatus;
use devo_protocol::native::session::MemorySetting;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use thiserror::Error;

const MEMORY_DATABASE_FILENAME: &str = "memory.sqlite3";
const MEMORY_SCHEMA_VERSION: &str = "2";

/// Per-session memory behavior kept by the server runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionMemorySettings {
    pub(crate) recall: MemorySetting,
    pub(crate) contribution: MemorySetting,
}

impl Default for SessionMemorySettings {
    fn default() -> Self {
        Self {
            recall: MemorySetting::Inherit,
            contribution: MemorySetting::Inherit,
        }
    }
}

/// A session memory setting snapshot returned by the actor for metadata updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionMemorySettingsSnapshot {
    pub(crate) settings: SessionMemorySettings,
    pub(crate) version: u64,
}

/// Errors raised by memory initialization or command execution.
#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("failed to prepare memory directory: {0}")]
    Directory(#[from] std::io::Error),
    #[error("memory database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("memory database lock was poisoned")]
    LockPoisoned,
    #[error("memory database returned an invalid count: {0}")]
    InvalidCount(i64),
}

/// Server-owned runtime for General Persistent Memory.
pub struct MemoryRuntime {
    config: MemoryConfig,
    connection: Mutex<Connection>,
}

impl MemoryRuntime {
    /// Opens or creates the dedicated memory database and applies all
    /// idempotent schema migrations.
    pub fn open(memory_root: PathBuf, config: MemoryConfig) -> Result<Self, MemoryError> {
        fs::create_dir_all(&memory_root)?;
        let connection = Connection::open(memory_root.join(MEMORY_DATABASE_FILENAME))?;
        create_schema(&connection)?;
        Ok(Self {
            config,
            connection: Mutex::new(connection),
        })
    }

    /// Prepares memory context for a turn. The foundation slice returns an
    /// empty preparation while preserving the disabled-mode contract.
    pub async fn prepare_turn(
        &self,
        _request: PrepareMemoryRequest,
    ) -> Result<PreparedMemory, MemoryError> {
        Ok(PreparedMemory::default())
    }

    /// Accepts a session source for later extraction work. Disabled memory
    /// never queues a source.
    pub async fn enqueue_source(
        &self,
        _source: SessionMemorySource,
    ) -> Result<EnqueueOutcome, MemoryError> {
        Ok(EnqueueOutcome {
            accepted: self.config.enabled,
        })
    }

    /// Executes one memory command through the public runtime seam.
    pub async fn execute_command(
        &self,
        command: MemoryCommand,
    ) -> Result<MemoryCommandResult, MemoryError> {
        match command {
            MemoryCommand::Status => Ok(MemoryCommandResult::Status(self.status()?)),
        }
    }

    fn status(&self) -> Result<MemoryStatus, MemoryError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| MemoryError::LockPoisoned)?;
        Ok(MemoryStatus {
            enabled: self.config.enabled,
            storage_health: "healthy".into(),
            entry_count: count_rows(&connection, "SELECT COUNT(*) FROM memory_entries")?,
            candidate_count: count_rows(&connection, "SELECT COUNT(*) FROM memory_candidates")?,
            pending_job_count: count_rows(
                &connection,
                "SELECT COUNT(*) FROM memory_jobs WHERE state = 'pending'",
            )?,
            retrying_job_count: count_rows(
                &connection,
                "SELECT COUNT(*) FROM memory_jobs WHERE state = 'retrying'",
            )?,
            error_job_count: count_rows(
                &connection,
                "SELECT COUNT(*) FROM memory_jobs WHERE state = 'error'",
            )?,
        })
    }
}

/// Commands supported by the memory runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryCommand {
    /// Return effective feature state and safe aggregate health counts.
    Status,
}

/// Result returned by [`MemoryRuntime::execute_command`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryCommandResult {
    /// Result of [`MemoryCommand::Status`].
    Status(MemoryStatus),
}

/// Input for turn preparation.
#[derive(Debug, Clone, Default)]
pub struct PrepareMemoryRequest {}

/// Prepared memory context for a turn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PreparedMemory {}

/// A completed session source eligible for future memory extraction.
#[derive(Debug, Clone, Default)]
pub struct SessionMemorySource {}

/// Outcome of attempting to enqueue a session source.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EnqueueOutcome {
    /// Whether this source was accepted for processing.
    pub accepted: bool,
}

fn count_rows(connection: &Connection, sql: &str) -> Result<u64, MemoryError> {
    let count: i64 = connection.query_row(sql, [], |row| row.get(0))?;
    u64::try_from(count).map_err(|_| MemoryError::InvalidCount(count))
}

fn create_schema(connection: &Connection) -> Result<(), MemoryError> {
    connection.execute_batch(
        "
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS memory_schema_meta (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
        );
        INSERT INTO memory_schema_meta (key, value)
        VALUES ('schema_version', '1')
        ON CONFLICT(key) DO NOTHING;

        CREATE TABLE IF NOT EXISTS memory_entries (
            entry_id TEXT PRIMARY KEY NOT NULL,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            normalized_key TEXT NOT NULL,
            body TEXT NOT NULL,
            origin TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_recalled_at TEXT,
            replacement_entry_id TEXT,
            expires_at TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS memory_entries_scope_key
            ON memory_entries (scope_type, scope_id, normalized_key);

        CREATE TABLE IF NOT EXISTS memory_candidates (
            candidate_id TEXT PRIMARY KEY NOT NULL,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            normalized_key TEXT NOT NULL,
            body TEXT NOT NULL,
            origin TEXT NOT NULL,
            source_session_id TEXT NOT NULL,
            validation_outcome TEXT,
            retention_until TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS memory_evidence (
            evidence_id TEXT PRIMARY KEY NOT NULL,
            entry_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            turn_id TEXT,
            observed_at TEXT NOT NULL,
            source_watermark TEXT NOT NULL,
            FOREIGN KEY(entry_id) REFERENCES memory_entries(entry_id)
        );

        CREATE TABLE IF NOT EXISTS memory_revocations (
            revocation_id TEXT PRIMARY KEY NOT NULL,
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            normalized_key TEXT NOT NULL,
            revoked_at TEXT NOT NULL,
            restored_at TEXT
        );

        CREATE TABLE IF NOT EXISTS memory_jobs (
            job_id TEXT PRIMARY KEY NOT NULL,
            job_kind TEXT NOT NULL DEFAULT 'source_scan',
            job_key TEXT NOT NULL DEFAULT '',
            source_session_id TEXT NOT NULL,
            source_watermark TEXT NOT NULL,
            state TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            lease_until TEXT,
            lease_owner TEXT,
            claimed_at TEXT,
            retry_at TEXT,
            error_class TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(source_session_id, source_watermark)
        );

        CREATE TABLE IF NOT EXISTS memory_scope_state (
            scope_type TEXT NOT NULL,
            scope_id TEXT NOT NULL,
            projection_revision INTEGER NOT NULL DEFAULT 0,
            ignore_sources_before TEXT,
            last_rebuild_at TEXT,
            PRIMARY KEY(scope_type, scope_id)
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS memory_entries_fts USING fts5(
            entry_id UNINDEXED,
            normalized_key,
            body
        );
        ",
    )?;
    migrate_schema(connection)?;
    Ok(())
}

fn migrate_schema(connection: &Connection) -> Result<(), MemoryError> {
    ensure_column(
        connection,
        "memory_jobs",
        "job_kind",
        "TEXT NOT NULL DEFAULT 'source_scan'",
    )?;
    ensure_column(
        connection,
        "memory_jobs",
        "job_key",
        "TEXT NOT NULL DEFAULT ''",
    )?;
    ensure_column(connection, "memory_jobs", "lease_owner", "TEXT")?;
    ensure_column(connection, "memory_jobs", "claimed_at", "TEXT")?;
    connection.execute(
        "UPDATE memory_jobs
         SET job_key = source_session_id || ':' || source_watermark
         WHERE job_key = ''",
        [],
    )?;
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS memory_jobs_kind_key
         ON memory_jobs (job_kind, job_key)",
        [],
    )?;
    connection.execute(
        "INSERT INTO memory_schema_meta (key, value)
         VALUES ('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [MEMORY_SCHEMA_VERSION],
    )?;
    Ok(())
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> Result<(), MemoryError> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM pragma_table_info(?1) WHERE name = ?2",
            rusqlite::params![table, column],
            |row| row.get::<_, i64>(0),
        )
        .optional()?
        .is_some();
    if !exists {
        connection.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
            [],
        )?;
    }
    Ok(())
}
