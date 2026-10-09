use super::*;

/// Trace: L2-DES-MEM-001 Rev 4 Background Scheduling and Failure Policy.
/// Verifies: an unknown explicit extraction model yields a visible terminal error without invoking a provider.
#[tokio::test]
async fn scan_reports_unknown_extraction_model() -> Result<()> {
    let (root, runtime, provider) = setup(/*sources*/ 1, /*permits*/ 0)?;
    let memory = Arc::new(crate::memory::MemoryRuntime::open(
        root.path().join("memory"),
        devo_core::MemoryConfig {
            enabled: true,
            extract_model: Some("missing/aux".into()),
            ..devo_core::MemoryConfig::default()
        },
    )?);
    for _ in 0..2 {
        let context = crate::memory::scan::ScanContext {
            trigger: ScanTrigger::SessionStart,
            db: Arc::clone(&runtime.deps.db),
            model_context: runtime.deps.context_for_workspace(root.path()).await?,
            usage_ledger: runtime.usage_ledger.clone(),
            triggering_session: SessionId::new(),
            activity: Arc::new(IdleSources),
        };
        Arc::clone(&memory).run_background_scan(context).await?;
    }
    let connection = connect(&runtime).await?;
    let response = runtime
        .handle_incoming(
            connection,
            serde_json::json!({"id":2,"method":"memory/status","params":{}}),
        )
        .await
        .context("memory status")?;
    pretty_assertions::assert_eq!(response["result"]["errorJobCount"], 1);
    pretty_assertions::assert_eq!(
        response["result"]["errorClasses"],
        serde_json::json!(["provider_unavailable"])
    );
    pretty_assertions::assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
    let ping = runtime
        .handle_incoming(
            connection,
            serde_json::json!({"id":3,"method":"runtime/ping","params":{}}),
        )
        .await
        .context("ping")?;
    assert!(ping.get("result").is_some());
    Ok(())
}
