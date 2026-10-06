//! Path-scoped, one-shot source-read interleavings for deterministic tests.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ReadPoint {
    AfterMetadata,
    BeforeTranscript,
    Complete,
}

struct Hook {
    skip_reads: usize,
    action: Box<dyn FnOnce() + Send>,
}

static HOOKS: LazyLock<Mutex<HashMap<(PathBuf, ReadPoint), Hook>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) struct ReadHookGuard(PathBuf, ReadPoint);

impl Drop for ReadHookGuard {
    fn drop(&mut self) {
        HOOKS.lock().unwrap().remove(&(self.0.clone(), self.1));
    }
}

pub(crate) fn on_read(
    path: &Path,
    point: ReadPoint,
    skip_reads: usize,
    action: impl FnOnce() + Send + 'static,
) -> ReadHookGuard {
    let previous = HOOKS.lock().unwrap().insert(
        (path.to_path_buf(), point),
        Hook {
            skip_reads,
            action: Box::new(action),
        },
    );
    assert!(previous.is_none(), "source read hook already registered");
    ReadHookGuard(path.to_path_buf(), point)
}

pub(super) fn run(path: &Path, point: ReadPoint) {
    let action = {
        let mut hooks = HOOKS.lock().unwrap();
        let key = (path.to_path_buf(), point);
        let Some(hook) = hooks.get_mut(&key) else {
            return;
        };
        if hook.skip_reads > 0 {
            hook.skip_reads -= 1;
            return;
        }
        hooks.remove(&key).unwrap().action
    };
    // The action may write a journal or install another hook: drop the map lock.
    action();
}
