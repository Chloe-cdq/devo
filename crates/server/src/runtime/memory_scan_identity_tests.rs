use super::*;
use pretty_assertions::assert_eq;

/// Trace: L1-REQ-MEM-001, L2-DES-MEM-001 Rev 4 Operational Scheduling.
/// Verifies: automation creation cannot spend extraction quota, and the same source remains available to an interactive start.
#[tokio::test]
async fn automation_start_does_not_schedule_extraction() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let connection = connect(&runtime).await?;
    let started = runtime
        .handle_incoming(
            connection,
            serde_json::json!({
                "id":1,"method":"session/new","params":{
                    "cwd":root.path(),"idempotencyKey":"automation","source":"automation"
                }
            }),
        )
        .await
        .context("automation session new")?;
    assert!(started.get("result").is_some());
    assert!(
        tokio::time::timeout(Duration::from_secs(1), provider.entered.notified())
            .await
            .is_err(),
        "automation creation triggered passive extraction"
    );
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);

    assert!(
        start(&runtime, connection, root.path())
            .await?
            .get("result")
            .is_some()
    );
    tokio::time::timeout(Duration::from_secs(5), provider.entered.notified()).await?;
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    provider.release.add_permits(1);
    Ok(())
}
