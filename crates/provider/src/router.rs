use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use devo_protocol::{ModelRequest, ProviderWireApi, StreamEvent};
use futures::{Stream, StreamExt};

use crate::error::ProviderError;
use crate::provider::ModelProviderSDK;

/// Identifies the configured provider that should handle one model request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProviderRoute {
    /// Use the default provider selected during server bootstrap.
    Default,
    /// Use a provider selected through a configured Connection.
    Connection {
        provider_id: String,
        wire_api: ProviderWireApi,
    },
}

impl ProviderRoute {
    pub fn connection(provider_id: impl Into<String>, wire_api: ProviderWireApi) -> Self {
        Self::Connection {
            provider_id: provider_id.into(),
            wire_api,
        }
    }
}

/// Server-facing facade for model provider invocation.
///
/// Per L3-DES-ARCH-001, `ProviderRouter` is the trait that server uses to
/// invoke model providers. It dispatches to the appropriate `ModelProviderSDK`
/// implementation based on the model profile.
///
/// The server should depend on `ProviderRouter` rather than on individual
/// provider SDK implementations directly.
#[async_trait]
pub trait ProviderRouter: Send + Sync {
    /// Send a streaming request to the appropriate provider.
    ///
    /// The router selects the correct provider adapter based on the model
    /// specified in the request, serializes the request, and returns a stream
    /// of normalized provider events.
    async fn stream(
        &self,
        route: ProviderRoute,
        request: ModelRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = anyhow::Result<StreamEvent>> + Send>>, ProviderError>;

    /// Send a non-streaming request to the appropriate provider.
    async fn complete(
        &self,
        route: ProviderRoute,
        request: ModelRequest,
    ) -> Result<devo_protocol::ModelResponse, ProviderError>;

    /// Known local initialization failure for the selected route, without I/O.
    /// None does not imply known quota or remote availability.
    fn initialization_error(&self, _route: &ProviderRoute) -> Option<ProviderError> {
        None
    }

    /// Latest available quota for the selected route; `None` means unavailable.
    fn remaining_quota_percent(&self, _route: &ProviderRoute) -> Option<u8> {
        None
    }

    /// Human-readable name of the router (e.g. "multi-provider", "openai-only").
    fn name(&self) -> &str;
}

/// A single-provider router that wraps a single `ModelProviderSDK`.
///
/// This is the simplest implementation of `ProviderRouter` for cases where
/// only one provider is configured.
pub struct SingleProviderRouter {
    provider: Arc<dyn ModelProviderSDK>,
}

impl SingleProviderRouter {
    pub fn new(provider: Arc<dyn ModelProviderSDK>) -> Self {
        Self { provider }
    }
}

#[async_trait]
impl ProviderRouter for SingleProviderRouter {
    async fn stream(
        &self,
        _route: ProviderRoute,
        request: ModelRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = anyhow::Result<StreamEvent>> + Send>>, ProviderError>
    {
        self.provider
            .completion_stream(request)
            .await
            .map(|stream| {
                Box::pin(stream.map(|item| item.map_err(crate::diagnostic::sanitize_error)))
                    as Pin<Box<dyn Stream<Item = anyhow::Result<StreamEvent>> + Send>>
            })
            .map_err(crate::diagnostic::normalize_error)
    }

    async fn complete(
        &self,
        _route: ProviderRoute,
        request: ModelRequest,
    ) -> Result<devo_protocol::ModelResponse, ProviderError> {
        self.provider
            .completion(request)
            .await
            .map_err(crate::diagnostic::normalize_error)
    }

    fn initialization_error(&self, _route: &ProviderRoute) -> Option<ProviderError> {
        self.provider.initialization_error()
    }

    fn remaining_quota_percent(&self, _route: &ProviderRoute) -> Option<u8> {
        self.provider.remaining_quota_percent()
    }

    fn name(&self) -> &str {
        "single-provider"
    }
}

/// Route-aware provider router backed by configured provider SDK instances.
pub struct MultiProviderRouter {
    default_provider: Arc<dyn ModelProviderSDK>,
    providers: HashMap<ProviderRoute, Arc<dyn ModelProviderSDK>>,
}

impl MultiProviderRouter {
    pub fn new(default_provider: Arc<dyn ModelProviderSDK>) -> Self {
        Self {
            default_provider,
            providers: HashMap::new(),
        }
    }

    pub fn insert_route(&mut self, route: ProviderRoute, provider: Arc<dyn ModelProviderSDK>) {
        self.providers.insert(route, provider);
    }

    fn provider_for_route(
        &self,
        route: &ProviderRoute,
    ) -> Result<&dyn ModelProviderSDK, ProviderError> {
        // The router owns providers for its lifetime, so dispatch can borrow the
        // selected adapter instead of cloning an Arc for each request.
        match route {
            ProviderRoute::Default => Ok(self.default_provider.as_ref()),
            ProviderRoute::Connection { provider_id, wire_api } => self
                .providers
                .get(route)
                .map(Arc::as_ref)
                .ok_or_else(|| ProviderError::UnknownError {
                    message: format!(
                        "provider route not configured: provider `{provider_id}` with wire API `{wire_api}`"
                    ).into(),
                    status_code: None,
                }),
        }
    }
}

#[async_trait]
impl ProviderRouter for MultiProviderRouter {
    async fn stream(
        &self,
        route: ProviderRoute,
        request: ModelRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = anyhow::Result<StreamEvent>> + Send>>, ProviderError>
    {
        self.provider_for_route(&route)?
            .completion_stream(request)
            .await
            .map(|stream| {
                Box::pin(stream.map(|item| item.map_err(crate::diagnostic::sanitize_error)))
                    as Pin<Box<dyn Stream<Item = anyhow::Result<StreamEvent>> + Send>>
            })
            .map_err(crate::diagnostic::normalize_error)
    }

    async fn complete(
        &self,
        route: ProviderRoute,
        request: ModelRequest,
    ) -> Result<devo_protocol::ModelResponse, ProviderError> {
        self.provider_for_route(&route)?
            .completion(request)
            .await
            .map_err(crate::diagnostic::normalize_error)
    }

    fn initialization_error(&self, route: &ProviderRoute) -> Option<ProviderError> {
        match self.provider_for_route(route) {
            Ok(provider) => provider.initialization_error(),
            Err(error) => Some(error),
        }
    }

    fn remaining_quota_percent(&self, route: &ProviderRoute) -> Option<u8> {
        self.provider_for_route(route)
            .ok()?
            .remaining_quota_percent()
    }

    fn name(&self) -> &str {
        "multi-provider"
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use devo_protocol::{
        ModelResponse, RequestContent, RequestMessage, ResponseContent, ResponseMetadata,
        StopReason, Usage,
    };
    use futures::stream;
    use pretty_assertions::assert_eq;

    use super::*;

    #[derive(Default)]
    struct CapturingProvider {
        requests: Mutex<Vec<String>>,
        quota: Option<u8>,
    }

    impl CapturingProvider {
        fn requests(&self) -> Vec<String> {
            self.requests
                .lock()
                .expect("capturing provider requests mutex should not be poisoned")
                .clone()
        }
    }

    #[async_trait]
    impl ModelProviderSDK for CapturingProvider {
        async fn completion(&self, request: ModelRequest) -> anyhow::Result<ModelResponse> {
            self.requests
                .lock()
                .expect("capturing provider requests mutex should not be poisoned")
                .push(request.model);
            Ok(ModelResponse {
                id: "response".to_string(),
                content: vec![ResponseContent::Text("ok".to_string())],
                stop_reason: Some(StopReason::EndTurn),
                usage: Usage::default(),
                metadata: ResponseMetadata::default(),
            })
        }

        async fn completion_stream(
            &self,
            request: ModelRequest,
        ) -> anyhow::Result<Pin<Box<dyn Stream<Item = anyhow::Result<StreamEvent>> + Send>>>
        {
            self.requests
                .lock()
                .expect("capturing provider requests mutex should not be poisoned")
                .push(request.model);
            Ok(Box::pin(stream::empty()))
        }

        fn remaining_quota_percent(&self) -> Option<u8> {
            self.quota
        }

        fn name(&self) -> &str {
            "capturing-provider"
        }
    }

    /// Trace: L2-DES-MEM-001 Rev 4.
    /// Verifies: router quota uses selected route and keeps missing routes unavailable.
    #[test]
    fn router_quota_uses_selected_route_and_keeps_missing_routes_unavailable() {
        let default = Arc::new(CapturingProvider {
            quota: Some(90),
            ..Default::default()
        });
        let selected = Arc::new(CapturingProvider {
            quota: Some(35),
            ..Default::default()
        });
        let mut router = MultiProviderRouter::new(default);
        let selected_route =
            ProviderRoute::connection("selected", ProviderWireApi::AnthropicMessages);
        router.insert_route(selected_route.clone(), selected);
        let missing = ProviderRoute::connection("missing", ProviderWireApi::AnthropicMessages);
        assert_eq!(
            [
                router.remaining_quota_percent(&ProviderRoute::Default),
                router.remaining_quota_percent(&selected_route),
                router.remaining_quota_percent(&missing),
            ],
            [Some(90), Some(35), None],
        );
    }

    /// Trace: L2-DES-MEM-001 Rev 4.
    /// Verifies: single router quota matches its route compatibility.
    #[test]
    fn single_router_quota_matches_its_route_compatibility() {
        let provider = Arc::new(CapturingProvider {
            quota: Some(35),
            ..Default::default()
        });
        let router = SingleProviderRouter::new(provider);
        assert_eq!(
            router.remaining_quota_percent(&ProviderRoute::connection(
                "other",
                ProviderWireApi::OpenAIResponses
            )),
            Some(35),
        );
    }
    fn request(model: &str) -> ModelRequest {
        ModelRequest {
            model_slug: devo_protocol::ModelProfileKey::Generic,
            model: model.to_string(),
            system: None,
            messages: vec![RequestMessage {
                role: "user".to_string(),
                content: vec![RequestContent::Text {
                    text: "hello".to_string(),
                }],
            }],
            max_tokens: 16,
            tools: None,
            hosted_tools: Vec::new(),
            sampling: Default::default(),
            request_thinking: None,
            reasoning_effort: None,
            extra_body: None,
        }
    }

    #[tokio::test]
    async fn multi_provider_router_dispatches_to_binding_route() {
        let default = Arc::new(CapturingProvider::default());
        let selected = Arc::new(CapturingProvider::default());
        let mut router = MultiProviderRouter::new(default.clone());
        router.insert_route(
            ProviderRoute::connection("openrouter", ProviderWireApi::OpenAIChatCompletions),
            selected.clone(),
        );

        router
            .complete(
                ProviderRoute::connection("openrouter", ProviderWireApi::OpenAIChatCompletions),
                request("vendor/model"),
            )
            .await
            .expect("route request");

        assert_eq!(default.requests(), Vec::<String>::new());
        assert_eq!(selected.requests(), vec!["vendor/model".to_string()]);
    }

    #[tokio::test]
    async fn multi_provider_router_reports_missing_binding_route() {
        let default = Arc::new(CapturingProvider::default());
        let router = MultiProviderRouter::new(default);

        let error = router
            .complete(
                ProviderRoute::connection("missing", ProviderWireApi::AnthropicMessages),
                request("claude"),
            )
            .await
            .expect_err("missing route should fail");

        assert_eq!(
            error.user_message(),
            "unknown provider error: provider route not configured: provider `missing` with wire API `anthropic_messages`"
        );
    }

    #[tokio::test]
    async fn single_provider_router_ignores_route_for_compatibility() {
        let provider = Arc::new(CapturingProvider::default());
        let router = SingleProviderRouter::new(provider.clone());

        router
            .complete(
                ProviderRoute::connection("other", ProviderWireApi::OpenAIResponses),
                request("any-model"),
            )
            .await
            .expect("single provider request");

        assert_eq!(provider.requests(), vec!["any-model".to_string()]);
    }
}
