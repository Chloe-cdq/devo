use super::*;
use crate::memory::source_read_test_support::{ReadPoint, on_read};
use pretty_assertions::assert_eq;

/// Trace: L2-DES-MEM-001 Rev 4 DD-7.
/// Verifies: an external marker appended after the captured length or header check cannot yield an eligible transcript.
#[test]
fn external_marker_during_source_read_rejects_captured_prefix() {
    for point in [ReadPoint::AfterMetadata, ReadPoint::BeforeTranscript] {
        let dir = TempDir::new().unwrap();
        let path = write_lines(&dir, &legacy(dir.path()));
        let store =
            crate::persistence::RolloutStore::new(dir.path().into(), /*event_log*/ None);
        let marker_path = path.clone();
        let _hook = on_read(&path, point, /*skip_reads*/ 0, move || {
            store
                .mark_external_context_used_at(&marker_path, SESSION.parse().unwrap())
                .unwrap();
        });
        assert_eq!(read_source(&path).unwrap(), None);
    }
}
