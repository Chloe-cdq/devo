use std::path::Path;
use std::path::PathBuf;

use devo_core::MemoryConfig;
use devo_protocol::native::rpc_memory::MemoryKind;
use devo_protocol::native::rpc_memory::MemoryScope;

use super::test_support::test_source;
use super::{MemoryRememberRequest, MemoryRuntime};

pub fn remember_request(text: &str) -> MemoryRememberRequest {
    MemoryRememberRequest {
        text: text.to_owned(),
        scope: MemoryScope::User,
        kind: Some(MemoryKind::Preference),
        source: test_source(
            Some("user-item-1"),
            "session-1",
            Some("turn-1"),
            PathBuf::new(),
        ),
    }
}

pub fn open_runtime(root: &Path) -> MemoryRuntime {
    MemoryRuntime::open(
        root.to_path_buf(),
        MemoryConfig {
            enabled: true,
            ..MemoryConfig::default()
        },
    )
    .expect("memory runtime")
}
