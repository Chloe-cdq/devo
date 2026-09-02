use std::path::Path;
use std::path::PathBuf;

use sha2::Digest;
use sha2::Sha256;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProjectIdentitySource {
    GitCommonDirectory,
    WorkspaceRoot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProjectMemoryIdentity {
    pub(super) scope_id: String,
    pub(super) canonical_source: PathBuf,
    pub(super) source: ProjectIdentitySource,
}

#[derive(Debug, Error)]
pub(super) enum ProjectIdentityError {
    #[error("failed to resolve project identity path {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid linked-worktree gitdir file: {0}")]
    InvalidGitFile(PathBuf),
}

pub(super) fn resolve_project_memory_identity(
    workspace_root: &Path,
) -> Result<ProjectMemoryIdentity, ProjectIdentityError> {
    let canonical_workspace = canonicalize(workspace_root)?;
    let (canonical_source, source) = match find_git_entry(&canonical_workspace) {
        Some((repo_root, git_entry)) => (
            resolve_git_common_directory(&repo_root, &git_entry)?,
            ProjectIdentitySource::GitCommonDirectory,
        ),
        None => (canonical_workspace, ProjectIdentitySource::WorkspaceRoot),
    };
    let normalized = normalize_identity_path(&canonical_source);
    let scope_id = format!("{:x}", Sha256::digest(normalized));
    Ok(ProjectMemoryIdentity {
        scope_id,
        canonical_source,
        source,
    })
}

fn find_git_entry(workspace_root: &Path) -> Option<(PathBuf, PathBuf)> {
    workspace_root.ancestors().find_map(|root| {
        let git_entry = root.join(".git");
        git_entry.exists().then(|| (root.to_path_buf(), git_entry))
    })
}

fn resolve_git_common_directory(
    repo_root: &Path,
    git_entry: &Path,
) -> Result<PathBuf, ProjectIdentityError> {
    if git_entry.is_dir() {
        return canonicalize(git_entry);
    }
    let git_file =
        std::fs::read_to_string(git_entry).map_err(|source| ProjectIdentityError::Io {
            path: git_entry.to_path_buf(),
            source,
        })?;
    let git_dir = git_file
        .trim()
        .strip_prefix("gitdir:")
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| ProjectIdentityError::InvalidGitFile(git_entry.to_path_buf()))?;
    let git_dir = canonicalize(&resolve_relative(repo_root, Path::new(git_dir)))?;
    let common_dir_file = git_dir.join("commondir");
    if !common_dir_file.exists() {
        return Ok(git_dir);
    }
    let common_dir =
        std::fs::read_to_string(&common_dir_file).map_err(|source| ProjectIdentityError::Io {
            path: common_dir_file.clone(),
            source,
        })?;
    let common_dir = common_dir.trim();
    if common_dir.is_empty() {
        return Err(ProjectIdentityError::InvalidGitFile(common_dir_file));
    }
    canonicalize(&resolve_relative(&git_dir, Path::new(common_dir)))
}

fn resolve_relative(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn canonicalize(path: &Path) -> Result<PathBuf, ProjectIdentityError> {
    std::fs::canonicalize(path).map_err(|source| ProjectIdentityError::Io {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(unix)]
fn normalize_identity_path(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;

    path.as_os_str().as_bytes().to_vec()
}

#[cfg(windows)]
fn normalize_identity_path(path: &Path) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;

    const FORWARD_SLASH: u16 = b'/' as u16;
    const BACKSLASH: u16 = b'\\' as u16;
    const ASCII_UPPERCASE_START: u16 = b'A' as u16;
    const ASCII_UPPERCASE_END: u16 = b'Z' as u16;
    const ASCII_CASE_OFFSET: u16 = (b'a' - b'A') as u16;

    let mut normalized = Vec::new();
    for unit in path.as_os_str().encode_wide() {
        let unit = match unit {
            FORWARD_SLASH => BACKSLASH,
            ASCII_UPPERCASE_START..=ASCII_UPPERCASE_END => unit + ASCII_CASE_OFFSET,
            unit => unit,
        };
        normalized.extend_from_slice(&unit.to_le_bytes());
    }
    normalized
}

#[cfg(not(any(unix, windows)))]
fn normalize_identity_path(path: &Path) -> Vec<u8> {
    path.as_os_str().to_string_lossy().as_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// Trace: L2-DES-MEM-001 DD-3
    /// Verifies: a main checkout and linked worktree share the Git common-dir identity.
    #[test]
    fn linked_worktree_resolves_to_main_git_common_directory() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let main_root = temp.path().join("main");
        let common_dir = main_root.join(".git");
        let linked_root = temp.path().join("linked");
        let linked_git_dir = common_dir.join("worktrees").join("linked");
        std::fs::create_dir_all(&linked_git_dir).expect("create linked git dir");
        std::fs::create_dir_all(&linked_root).expect("create linked root");
        std::fs::write(linked_git_dir.join("commondir"), "../..\n").expect("write commondir");
        std::fs::write(
            linked_root.join(".git"),
            format!("gitdir: {}\n", linked_git_dir.display()),
        )
        .expect("write worktree git file");

        let main = resolve_project_memory_identity(&main_root).expect("main identity");
        let linked = resolve_project_memory_identity(&linked_root).expect("linked identity");
        let canonical_common = std::fs::canonicalize(&common_dir).expect("canonical common dir");

        assert_eq!(main, linked);
        assert_eq!(main.source, ProjectIdentitySource::GitCommonDirectory);
        assert_eq!(main.canonical_source, canonical_common);
        assert_eq!(main.scope_id.len(), 64);
    }

    /// Trace: L2-DES-MEM-001 DD-3
    /// Verifies: non-Git workspaces use their canonical workspace root as identity input.
    #[test]
    fn non_git_workspace_uses_canonical_workspace_root() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("create workspace");

        let identity = resolve_project_memory_identity(&workspace).expect("workspace identity");

        assert_eq!(identity.source, ProjectIdentitySource::WorkspaceRoot);
        assert_eq!(
            identity.canonical_source,
            std::fs::canonicalize(workspace).expect("canonical workspace")
        );
        assert_eq!(identity.scope_id.len(), 64);
    }

    /// Verifies: distinct Windows-native path values never collapse through lossy UTF-8 conversion.
    #[cfg(windows)]
    #[test]
    fn identity_bytes_distinguish_unpaired_utf16() {
        use std::ffi::OsString;
        use std::os::windows::ffi::OsStringExt;

        let left = PathBuf::from(OsString::from_wide(&[0xd800]));
        let right = PathBuf::from(OsString::from_wide(&[0xd801]));

        assert_ne!(
            normalize_identity_path(&left),
            normalize_identity_path(&right)
        );
    }

    /// Verifies: on Unix, a literal backslash and a path separator produce distinct scopes.
    #[cfg(unix)]
    #[test]
    fn identity_distinguishes_backslash_from_path_separator() {
        let temp = tempfile::TempDir::new().expect("temp dir");
        let backslash = temp.path().join("a\\b");
        let separator = temp.path().join("a").join("b");
        std::fs::create_dir_all(&backslash).expect("create backslash path");
        std::fs::create_dir_all(&separator).expect("create separator path");

        let backslash = resolve_project_memory_identity(&backslash).expect("backslash identity");
        let separator = resolve_project_memory_identity(&separator).expect("separator identity");

        assert_ne!(backslash.scope_id, separator.scope_id);
    }

    /// Verifies: on Unix, distinct non-UTF-8 names produce distinct scopes.
    #[cfg(unix)]
    #[test]
    fn identity_distinguishes_non_utf8_paths() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let temp = tempfile::TempDir::new().expect("temp dir");
        let left = temp
            .path()
            .join(OsString::from_vec(b"project-\xff".to_vec()));
        let right = temp
            .path()
            .join(OsString::from_vec(b"project-\xfe".to_vec()));
        std::fs::create_dir_all(&left).expect("create left path");
        std::fs::create_dir_all(&right).expect("create right path");

        let left = resolve_project_memory_identity(&left).expect("left identity");
        let right = resolve_project_memory_identity(&right).expect("right identity");

        assert_ne!(left.scope_id, right.scope_id);
    }
}
