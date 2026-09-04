use rusqlite::{Connection, OptionalExtension};

use super::{MEMORY_SCHEMA_VERSION, MemoryError};

pub(super) fn create_schema(connection: &Connection) -> Result<(), MemoryError> {
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
            source_user_item_id TEXT,
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
    let transaction = connection.unchecked_transaction()?;
    ensure_column(
        &transaction,
        "memory_jobs",
        "job_kind",
        "TEXT NOT NULL DEFAULT 'source_scan'",
    )?;
    ensure_column(
        &transaction,
        "memory_jobs",
        "job_key",
        "TEXT NOT NULL DEFAULT ''",
    )?;
    ensure_column(&transaction, "memory_jobs", "lease_owner", "TEXT")?;
    ensure_column(&transaction, "memory_jobs", "claimed_at", "TEXT")?;
    ensure_column(
        &transaction,
        "memory_evidence",
        "source_user_item_id",
        "TEXT",
    )?;
    transaction.execute(
        "UPDATE memory_jobs
         SET job_key = source_session_id || ':' || source_watermark
         WHERE job_key = ''",
        [],
    )?;
    transaction.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS memory_jobs_kind_key
         ON memory_jobs (job_kind, job_key)",
        [],
    )?;
    transaction.execute_batch(
        "UPDATE memory_revocations AS kept
         SET revoked_at = (
                 SELECT MAX(all_rows.revoked_at)
                 FROM memory_revocations AS all_rows
                 WHERE all_rows.scope_type = kept.scope_type
                   AND all_rows.scope_id = kept.scope_id
                   AND all_rows.normalized_key = kept.normalized_key
             ),
             restored_at = (
                 SELECT CASE
                     WHEN MAX(all_rows.restored_at) >= MAX(all_rows.revoked_at)
                     THEN MAX(all_rows.restored_at)
                     ELSE NULL
                 END
                 FROM memory_revocations AS all_rows
                 WHERE all_rows.scope_type = kept.scope_type
                   AND all_rows.scope_id = kept.scope_id
                   AND all_rows.normalized_key = kept.normalized_key
             )
         WHERE kept.revocation_id = (
             SELECT MAX(candidate.revocation_id)
             FROM memory_revocations AS candidate
             WHERE candidate.scope_type = kept.scope_type
               AND candidate.scope_id = kept.scope_id
               AND candidate.normalized_key = kept.normalized_key
         );

         DELETE FROM memory_revocations
         WHERE revocation_id != (
             SELECT MAX(candidate.revocation_id)
             FROM memory_revocations AS candidate
             WHERE candidate.scope_type = memory_revocations.scope_type
               AND candidate.scope_id = memory_revocations.scope_id
               AND candidate.normalized_key = memory_revocations.normalized_key
         );",
    )?;
    transaction.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS memory_revocations_scope_identity
         ON memory_revocations (scope_type, scope_id, normalized_key)",
        [],
    )?;
    transaction.execute(
        "INSERT INTO memory_schema_meta (key, value)
         VALUES ('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [MEMORY_SCHEMA_VERSION],
    )?;
    transaction.commit()?;
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
