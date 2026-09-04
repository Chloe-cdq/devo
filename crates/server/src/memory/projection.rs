use std::fs;
use std::io::Write;
use std::path::Path;

use devo_protocol::native::rpc_memory::MemoryEntry;
use devo_protocol::native::rpc_memory::MemoryKind;
use devo_protocol::native::rpc_memory::MemoryScope;

use super::MemoryError;

pub(super) fn render_projection(scope: MemoryScope, entries: &[MemoryEntry]) -> String {
    let title = match scope {
        MemoryScope::User => "User",
        MemoryScope::Project => "Project",
    };
    let mut projection = format!(
        "# {title} Memory\n\n<!-- Generated from SQLite. Read-only; manual edits are not canonical. -->\n"
    );
    if entries.is_empty() {
        projection.push_str("\n_No memory entries._\n");
        return projection;
    }
    projection.push('\n');
    for entry in entries {
        let kind = match entry.kind {
            MemoryKind::Preference => "preference",
            MemoryKind::Feedback => "feedback",
            MemoryKind::Fact => "fact",
            MemoryKind::Reference => "reference",
        };
        let entry_id = entry.entry_id.to_string();
        let body = &entry.body;
        projection.push_str(&format!("\n- **{kind}** `{entry_id}` — {body}\n"));
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
