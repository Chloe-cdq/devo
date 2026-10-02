use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;

use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_protocol::native::rpc_memory::MemoryScope;

use super::entries::contains_secret;
use super::{MemoryError, kind_name, origin_name, state_name};

pub(super) fn render_projection(scope: MemoryScope, entries: &[MemoryEntry]) -> String {
    let title = match scope {
        MemoryScope::User => "User",
        MemoryScope::Project => "Project",
    };
    let mut projection = format!(
        "# {title} Memory\n\n<!-- Generated from SQLite. Read-only; manual edits are not canonical. -->\n"
    );
    let entries = entries
        .iter()
        .filter(|entry| !contains_secret(&entry.body) && !contains_secret(&entry.normalized_key))
        .collect::<Vec<_>>();
    if entries.is_empty() {
        projection.push_str("\n_No memory entries._\n");
        return projection;
    }
    let mut entries_by_kind = BTreeMap::<&str, Vec<&MemoryEntry>>::new();
    for entry in entries {
        entries_by_kind
            .entry(kind_name(entry.kind))
            .or_default()
            .push(entry);
    }
    for (kind, entries) in entries_by_kind {
        projection.push_str(&format!("\n## {kind}\n"));
        for entry in entries {
            projection.push_str(&format!(
                "\n- `{}` — {}\n  - state: {}\n  - origin: {}\n  - created_at: {}\n  - updated_at: {}\n",
                entry.entry_id,
                entry.body,
                state_name(entry.state),
                origin_name(entry.origin),
                entry.created_at.to_rfc3339(),
                entry.updated_at.to_rfc3339(),
            ));
            if !entry.provenance.is_empty() {
                projection.push_str("  - sources:\n");
                for source in &entry.provenance {
                    let source_user_item_id = source
                        .source_user_item_id
                        .as_ref()
                        .map_or_else(|| "<none>".to_string(), ToString::to_string);
                    projection.push_str(&format!(
                        "    - source_session_id: {}\n      source_turn_id: {}\n      source_user_item_id: {}\n",
                        source.source_session_id.as_deref().unwrap_or("<none>"),
                        source.source_turn_id.as_deref().unwrap_or("<none>"),
                        source_user_item_id,
                    ));
                }
            }
        }
    }
    projection
}

pub(super) fn write_atomic_projection(path: &Path, data: &[u8]) -> Result<(), MemoryError> {
    let parent = path
        .parent()
        .ok_or_else(|| MemoryError::InvalidRequest("memory projection has no parent".into()))?;
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("MEMORY.md");
    for attempt in 0..16 {
        let temporary = parent.join(format!(".{file_name}.{}.{attempt}.tmp", std::process::id()));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(mut file) => {
                if let Err(error) = file.write_all(data).and_then(|()| file.sync_all()) {
                    let _ = fs::remove_file(&temporary);
                    return Err(MemoryError::Directory(error));
                }
                if let Err(error) = replace_projection(&temporary, path) {
                    let _ = fs::remove_file(&temporary);
                    return Err(MemoryError::Directory(error));
                }
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(MemoryError::Directory(error)),
        }
    }
    Err(MemoryError::InvalidRequest(format!(
        "failed to create memory projection temporary file after 16 attempts in {}",
        parent.display()
    )))
}

#[cfg(not(windows))]
fn replace_projection(temporary: &Path, target: &Path) -> std::io::Result<()> {
    fs::rename(temporary, target)
}

#[cfg(windows)]
fn replace_projection(temporary: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let temporary = temporary
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let result = unsafe {
        MoveFileExW(
            temporary.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::render_projection;
    use devo_protocol::native::ids::MemoryEntryId;
    use devo_protocol::native::rpc_memory::{
        MemoryEntry, MemoryKind, MemoryOrigin, MemoryScope, MemoryState,
    };
    use pretty_assertions::assert_eq;

    /// Trace: L2-DES-MEM-001 DD-6
    /// Verifies: unsafe historical entries cannot appear in a projection regardless of lifecycle state.
    #[test]
    fn credential_projection_excludes_unsafe_entries_in_every_state() {
        let timestamp = "2026-09-28T00:00:00Z".parse().unwrap();
        let safe = MemoryEntry {
            entry_id: MemoryEntryId::from_string("mem_safe".into()),
            scope: MemoryScope::User,
            scope_id: "user".into(),
            kind: MemoryKind::Fact,
            normalized_key: "rust".into(),
            body: "Use Rust".into(),
            origin: MemoryOrigin::ExplicitUser,
            state: MemoryState::Active,
            created_at: timestamp,
            updated_at: timestamp,
            replacement_entry_id: None,
            provenance: vec![],
        };
        let expected = "# User Memory\n\n<!-- Generated from SQLite. Read-only; manual edits are not canonical. -->\n\n## fact\n\n- `mem_safe` — Use Rust\n  - state: active\n  - origin: explicit_user\n  - created_at: 2026-09-28T00:00:00+00:00\n  - updated_at: 2026-09-28T00:00:00+00:00\n";
        let empty = "# User Memory\n\n<!-- Generated from SQLite. Read-only; manual edits are not canonical. -->\n\n_No memory entries._\n";
        for state in [
            MemoryState::Active,
            MemoryState::Restored,
            MemoryState::Stale,
            MemoryState::Retired,
            MemoryState::Conflicted,
        ] {
            for field in ["body", "key"] {
                let mut unsafe_entry = safe.clone();
                unsafe_entry.entry_id = MemoryEntryId::from_string("mem_unsafe".into());
                unsafe_entry.state = state;
                match field {
                    "body" => unsafe_entry.body = "API key: ab".into(),
                    "key" => unsafe_entry.normalized_key = "API key: ab".into(),
                    _ => unreachable!(),
                }
                assert_eq!(
                    render_projection(MemoryScope::User, &[safe.clone(), unsafe_entry.clone()]),
                    expected
                );
                assert_eq!(render_projection(MemoryScope::User, &[unsafe_entry]), empty);
            }
        }
    }
}
