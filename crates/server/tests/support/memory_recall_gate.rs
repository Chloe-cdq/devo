use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use devo_protocol::{ModelRequest, ModelResponse, StreamEvent};
use devo_provider::ModelProviderSDK;
use futures::{Stream, StreamExt};
use tokio::sync::{Semaphore, mpsc};

use crate::support::ScriptedProvider;

pub struct GatedProvider {
    pub inner: ScriptedProvider,
    pub requests: mpsc::UnboundedSender<ModelRequest>,
    pub release: Arc<Semaphore>,
}

#[async_trait]
impl ModelProviderSDK for GatedProvider {
    async fn completion(&self, request: ModelRequest) -> Result<ModelResponse> {
        self.inner.completion(request).await
    }

    async fn completion_stream(
        &self,
        request: ModelRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamEvent>> + Send>>> {
        self.requests.send(request.clone())?;
        let stream = self.inner.completion_stream(request).await?;
        let release = self.release.clone();
        let gate = futures::stream::once(async move {
            release
                .acquire_owned()
                .await
                .expect("test gate open")
                .forget();
            None::<Result<StreamEvent>>
        })
        .filter_map(futures::future::ready);
        Ok(Box::pin(gate.chain(stream)))
    }

    fn name(&self) -> &str {
        "recall-gated-provider"
    }
}
