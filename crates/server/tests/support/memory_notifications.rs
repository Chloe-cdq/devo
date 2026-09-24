use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use tokio::sync::mpsc;

pub async fn wait_for_item_completed(
    notifications: &mut mpsc::Receiver<serde_json::Value>,
    item_id: &str,
) -> Result<()> {
    loop {
        let notification =
            tokio::time::timeout(Duration::from_secs(/*seconds*/ 5), notifications.recv())
                .await?
                .context("item completion notification channel closed")?;
        if notification["method"] == "item/completed"
            && notification["params"]["item"]["id"] == item_id
        {
            return Ok(());
        }
    }
}
