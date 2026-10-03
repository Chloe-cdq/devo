use crate::memory::{runtime_test_support::open_runtime, schema};
use pretty_assertions::assert_eq;
use rusqlite::{Connection, types::Value};

fn seeded_v6() -> Connection {
    let connection = Connection::open_in_memory().unwrap();
    schema::create_schema(&connection).unwrap();
    connection.execute_batch(
        "UPDATE memory_schema_meta SET value = '6' WHERE key = 'schema_version';
         INSERT INTO memory_entries
            (entry_id,scope_type,scope_id,kind,normalized_key,body,origin,state,created_at,updated_at,replacement_entry_id)
         VALUES
            ('safe','user','user','fact','use rust','use rust','explicit_user','active','2026-09-28T00:00:00Z','2026-09-28T00:00:00Z',NULL),
            ('safe_ref','user','user','fact','use tabs','Use tabs','inferred_session','retired','2026-09-28T00:00:00Z','2026-09-28T00:00:00Z','unsafe_body'),
            ('unsafe_body','user','user','fact','api key ab','API key: ab','inferred_session','conflicted','2026-09-28T00:00:00Z','2026-09-28T00:00:00Z',NULL),
            ('unsafe_key','user','user','fact','API key: keycopy','safe body','explicit_user','retired','2026-09-28T00:00:00Z','2026-09-28T00:00:00Z',NULL),
            ('unsafe_project','project','project-only','fact','project legacy identity','API key = projectcopy','inferred_session','active','2026-09-28T00:00:00Z','2026-09-28T00:00:00Z',NULL);
         INSERT INTO memory_candidates
            (candidate_id,scope_type,scope_id,kind,normalized_key,body,origin,source_session_id,retention_until,created_at)
         VALUES
            ('safe_candidate','user','user','fact','language','use rust','inferred_session','ses_safe','2027-01-01T00:00:00Z','2026-09-28T00:00:00Z'),
            ('unsafe_candidate_body','user','user','fact','safe-group','API key: candidatecopy','inferred_session','ses_unsafe','2027-01-01T00:00:00Z','2026-09-28T00:00:00Z'),
            ('unsafe_candidate_key','user','user','fact','API key: candidatekeycopy','safe candidate body','inferred_session','ses_unsafe','2027-01-01T00:00:00Z','2026-09-28T00:00:00Z');
         INSERT INTO memory_evidence
            (evidence_id,entry_id,session_id,observed_at,source_watermark)
         VALUES
            ('safe_evidence','safe','ses_safe','2026-09-28T00:00:00Z','safe watermark'),
            ('unsafe_evidence','unsafe_body','ses_unsafe','2026-09-28T00:00:00Z','unsafe watermark');
         INSERT INTO memory_proposal_claims(scope_type,scope_id,proposal_key,canonical_key,entry_id)
         VALUES
            ('user','user','language','use rust','safe'),
            ('user','user','safe-group','api key ab','unsafe_body'),
            ('user','user','API key: proposalcopy','safe canonical claim',NULL),
            ('user','user','safe-group','API key: canonicalcopy',NULL),
            ('user','user','safe-group','api key candidatecopy',NULL),
            ('project','project-only','safe-group','project legacy identity','unsafe_project');
         INSERT INTO memory_revocations
            (revocation_id,scope_type,scope_id,normalized_key,revoked_at)
         VALUES
            ('safe_revocation','user','user','safe tombstone','2026-09-28T00:00:00Z'),
            ('unsafe_revocation','user','user','api key ab','2026-09-28T00:00:00Z'),
            ('unsafe_revocation_key','user','user','API key: revocationcopy','2026-09-28T00:00:00Z'),
            ('unsafe_candidate_revocation','user','user','api key candidatecopy','2026-09-28T00:00:00Z');
         INSERT INTO memory_entries_fts(entry_id,normalized_key,body)
         VALUES
            ('safe','use rust','use rust'),
            ('unsafe_body','api key ab','API key: ab'),
            ('unsafe_key','API key: keycopy','safe body'),
            ('orphan','orphan','API key: orphanftscopy');
         INSERT INTO memory_scope_state(scope_type,scope_id,projection_revision)
         VALUES ('user','user',12);"
    ).unwrap();
    connection
}

fn snapshot(connection: &Connection) -> Vec<(String, Vec<Vec<Value>>)> {
    [
        "memory_entries",
        "memory_candidates",
        "memory_evidence",
        "memory_proposal_claims",
        "memory_revocations",
        "memory_entries_fts",
        "memory_scope_state",
    ]
    .into_iter()
    .map(|table| {
        let columns = if table == "memory_proposal_claims" {
            "scope_type, scope_id, proposal_key, canonical_key, entry_id"
        } else {
            "*"
        };
        let mut statement = connection
            .prepare(&format!("SELECT {columns} FROM {table} ORDER BY rowid"))
            .unwrap();
        let columns = statement.column_count();
        let rows = statement
            .query_map([], |row| {
                (0..columns)
                    .map(|index| row.get(index))
                    .collect::<Result<Vec<Value>, _>>()
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        (table.into(), rows)
    })
    .collect()
}

fn assert_scrubbed(connection: &Connection, before: &[(String, Vec<Vec<Value>>)]) {
    let mut expected = before.to_vec();
    for (table, rows) in &mut expected {
        match table.as_str() {
            "memory_entries" => {
                rows.retain(
                    |row| matches!(&row[0], Value::Text(id) if id == "safe" || id == "safe_ref"),
                );
                rows[1][11] = Value::Null;
            }
            "memory_candidates" | "memory_evidence" | "memory_revocations"
            | "memory_entries_fts" => {
                rows.retain(|row| matches!(&row[0], Value::Text(id) if id.starts_with("safe")));
            }
            "memory_proposal_claims" => rows.retain(|row| row[2] == Value::Text("language".into())),
            "memory_scope_state" => rows.push(vec![
                Value::Text("project".into()),
                Value::Text("project-only".into()),
                Value::Integer(0),
                Value::Null,
                Value::Null,
            ]),
            _ => unreachable!(),
        }
    }
    assert_eq!(snapshot(connection), expected);
    let violations = connection
        .prepare("PRAGMA foreign_key_check")
        .unwrap()
        .query_map([], |_| Ok(()))
        .unwrap()
        .count();
    assert_eq!(violations, 0);
}

/// Trace: L2-DES-MEM-001 DD-6/DD-8
/// Verifies: v7 removes historical unsafe copies while retaining safe records and scope rebuild markers.
#[test]
fn credential_v6_migration_purges_sensitive_copies_and_preserves_safe_data() {
    let connection = seeded_v6();
    let before = snapshot(&connection);
    schema::create_schema(&connection).unwrap();
    assert_scrubbed(&connection, &before);
    let after = snapshot(&connection);
    schema::create_schema(&connection).unwrap();
    assert_eq!(snapshot(&connection), after);
}

/// Trace: L2-DES-MEM-001 DD-6/DD-8
/// Verifies: a failure at the final version write rolls back all sensitive cleanup and retries atomically.
#[test]
fn credential_v6_migration_rolls_back_and_retries_without_partial_cleanup() {
    let connection = seeded_v6();
    let before = snapshot(&connection);
    connection
        .execute_batch(
            "CREATE TRIGGER fail_version_write BEFORE UPDATE ON memory_schema_meta
         WHEN NEW.key = 'schema_version' AND NEW.value = '9'
         BEGIN SELECT RAISE(ABORT, 'version write blocked'); END;",
        )
        .unwrap();
    assert!(schema::create_schema(&connection).is_err());
    assert_eq!(snapshot(&connection), before);
    assert_eq!(
        connection
            .query_row(
                "SELECT value FROM memory_schema_meta WHERE key='schema_version'",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
        "6"
    );
    connection
        .execute_batch("DROP TRIGGER fail_version_write;")
        .unwrap();
    schema::create_schema(&connection).unwrap();
    assert_scrubbed(&connection, &before);
}

/// Trace: L2-DES-MEM-001 DD-6
/// Verifies: deleting the final unsafe entry still rewrites stale Markdown when the runtime reopens.
#[test]
fn credential_v6_migration_rebuilds_projection_for_an_emptied_scope() {
    let root = tempfile::tempdir().unwrap();
    let runtime = open_runtime(root.path());
    {
        let connection = runtime.connection.lock().unwrap();
        connection.execute_batch(
            "UPDATE memory_schema_meta SET value='6' WHERE key='schema_version';
             INSERT INTO memory_entries(entry_id,scope_type,scope_id,kind,normalized_key,body,origin,state,created_at,updated_at)
             VALUES('unsafe','user','user','fact','legacy api key','API key: oldcopy','inferred_session','retired','2026-09-28T00:00:00Z','2026-09-28T00:00:00Z');"
        ).unwrap();
    }
    std::fs::create_dir_all(root.path().join("user")).unwrap();
    std::fs::write(root.path().join("user/MEMORY.md"), "API key: oldcopy").unwrap();
    drop(runtime);
    let runtime = open_runtime(root.path());
    assert_eq!(
        runtime
            .list(crate::memory::ListMemoryRequest::default())
            .unwrap()
            .data,
        vec![]
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("user/MEMORY.md")).unwrap(),
        crate::memory::projection::render_projection(
            devo_protocol::native::rpc_memory::MemoryScope::User,
            &[]
        )
    );
}

/// Trace: L2-DES-MEM-001 DD-6/DD-8
/// Verifies: an unsafe stale FTS copy is replaced with surviving safe canonical bytes.
#[test]
fn credential_v6_migration_repairs_unsafe_fts_copy_of_a_safe_entry() {
    let connection = seeded_v6();
    connection
        .execute(
            "UPDATE memory_entries_fts SET body='API key: staleftscopy' WHERE entry_id='safe'",
            [],
        )
        .unwrap();
    schema::create_schema(&connection).unwrap();
    let rows = connection
        .prepare("SELECT entry_id,normalized_key,body FROM memory_entries_fts ORDER BY entry_id")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![("safe".into(), "use rust".into(), "use rust".into())]
    );
}

/// Trace: L2-DES-MEM-001 DD-6/DD-8/DD-9
/// Verifies: a lossy key derived from an unsafe candidate cannot erase surviving safe claim authority or revocation.
#[test]
fn credential_v6_migration_preserves_ambiguous_safe_legacy_key_authority() {
    let connection = seeded_v6();
    connection.execute_batch(
        "INSERT INTO memory_entries(entry_id,scope_type,scope_id,kind,normalized_key,body,origin,state,created_at,updated_at)
         VALUES('safe_collision','user','user','fact','api key candidatecopy','api key candidatecopy','explicit_user','retired','2026-09-28T00:00:00Z','2026-09-28T00:00:00Z');
         UPDATE memory_revocations SET revocation_id='safe_collision_revocation' WHERE revocation_id='unsafe_candidate_revocation';
         INSERT INTO memory_proposal_claims(scope_type,scope_id,proposal_key,canonical_key,entry_id)
         VALUES('user','user','safe-collision','api key candidatecopy','safe_collision');"
    ).unwrap();
    let before = snapshot(&connection);
    schema::create_schema(&connection).unwrap();
    let after = snapshot(&connection);
    for (table, index, identity) in [
        ("memory_entries", 0, "safe_collision"),
        ("memory_revocations", 0, "safe_collision_revocation"),
        ("memory_proposal_claims", 2, "safe-collision"),
    ] {
        let before_rows = before
            .iter()
            .find(|(name, _)| name == table)
            .unwrap()
            .1
            .iter()
            .filter(|row| row[index] == Value::Text(identity.into()))
            .cloned()
            .collect::<Vec<_>>();
        let after_rows = after
            .iter()
            .find(|(name, _)| name == table)
            .unwrap()
            .1
            .iter()
            .filter(|row| row[index] == Value::Text(identity.into()))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(after_rows, before_rows);
    }
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM memory_candidates WHERE candidate_id='unsafe_candidate_body'",
                [],
                |row| row.get::<_, u32>(0)
            )
            .unwrap(),
        0
    );
}
