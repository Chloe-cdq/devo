/// Hold the real storage mutex on an OS thread until the test releases it.
/// The deadline bounds a broken implementation without sleeping in the test.
pub(crate) fn hold_storage(
    memory: std::sync::Arc<super::MemoryRuntime>,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    std::sync::mpsc::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _storage = memory.connection.lock().unwrap();
        held_tx.send(()).unwrap();
        let _ = release_rx.recv_timeout(std::time::Duration::from_secs(/*secs*/ 30));
    });
    (held_rx, release_tx, worker)
}
