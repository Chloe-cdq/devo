# `devo-provider`

Shared provider layer for model invocation.

This crate defines:

- provider adapter traits and concrete provider implementations
- traits for provider SDK implementations
- concrete adapters for OpenAI-family and Anthropic-family transports
- provider capability metadata used by higher layers to shape requests

## What lives in this crate

The crate is organized around two main building blocks plus the provider-neutral
protocol dependency:

- `devo_protocol`
  Owns the normalized provider-agnostic model I/O IR:
  - `ModelRequest`
  - `RequestMessage`
  - `RequestContent`
  - `SamplingControls`
  - `ToolDefinition`
  - `RequestRole`
  - `ModelResponse`
  - `ResponseContent`
  - `Usage`
  - `ResponseMetadata`
  - `ResponseExtra`
  - `StreamEvent`
  - `StopReason`

- `request.rs`
  Defines provider-local request helpers such as extra-body merging.

- `provider.rs`
  Defines the provider abstraction:
  - `ModelProviderSDK`
  - `ProviderAdapter`
  - `ProviderFamily`
  - `ProviderCapabilities`

In short, higher layers construct a `devo_protocol::ModelRequest`, send it
through a `ModelProviderSDK`, and receive either a complete
`devo_protocol::ModelResponse` or a stream of `devo_protocol::StreamEvent`
values.

## Provider families

### OpenAI

The `openai` module contains OpenAI-family adapters and helpers.

Implemented transports:

- `openai::chat_completions::OpenAIProvider`
  OpenAI Chat Completions style transport.
  Reference:
  <https://developers.openai.com/api/reference/chat-completions/overview>

- `openai::responses::OpenAIResponsesProvider`
  OpenAI Responses API transport.
  Reference:
  <https://developers.openai.com/api/reference/resources/responses>

Supporting modules:

- `openai::capabilities`
  Resolves transport/model-specific request capability differences.

- `openai::reasoning_effort`
  Shared reasoning-effort enum for OpenAI-family transports.

- `openai::role`
  OpenAI wire-role definitions.

The OpenAI adapters parse the full provider payload into typed local structs,
map the shared fields into the crate IR, and preserve richer provider-specific
fields in `ResponseMetadata`.

### Anthropic

The `anthropic` module contains Anthropic-family adapters and helpers.

Implemented transport:

- `anthropic::messages::AnthropicProvider`
  Anthropic Messages API transport.
  Reference:
  <https://platform.claude.com/docs/en/api/messages>

Supporting modules:

- `anthropic::role`
  Anthropic wire-role definitions.

Like the OpenAI adapters, the Anthropic adapter builds typed request payloads,
parses typed response payloads, maps supported fields into the shared IR, and
preserves richer provider-specific data in response metadata.

## Normalized IR

The crate does not try to expose every vendor field directly in the shared API.
Instead it normalizes the pieces the rest of the system depends on:

- text output
- tool-use output
- stop reason
- token usage
- streaming deltas
- selected provider-specific metadata

When a provider returns additional fields that do not fit the shared IR cleanly,
they can be preserved through `ResponseExtra::ProviderSpecific`.

This keeps the common interface small while still allowing adapters to retain
important vendor-specific information.

## Capabilities

`ProviderCapabilities` describes differences between providers and transports,
for example:

- supported roles
- support for `temperature`
- support for `top_p`
- support for `top_k`
- support for reasoning-effort controls
- support for tool calls
- support for surfaced reasoning content

Higher layers can use these capability flags to shape requests before they are
serialized by a provider adapter.

## Quota telemetry

`ModelProviderSDK::remaining_quota_percent()` returns the latest available
remaining quota in `0..=100`. The default is `None`; unavailable telemetry never
implies full quota. `ProviderRouter::remaining_quota_percent(&route)` forwards
that observation for the selected adapter and returns `None` for missing routes.

OpenAI Chat Completions, OpenAI Responses, and Anthropic Messages capture quota
headers from complete and streaming responses, including HTTP failures. They
report the lowest known request/token window, round down, and clamp at 100.
Invalid or absent windows are unavailable. Starting another HTTP request clears
the preceding observation. Only the response for the latest-started request can
update it; delayed older responses cannot replace low or unavailable quota.
Observations expire after 60 seconds; adapters never infer that a reset has
replenished quota.

Header references: [OpenAI rate limits](https://developers.openai.com/api/docs/guides/rate-limits)
and [Anthropic rate limits](https://platform.claude.com/docs/en/api/rate-limits).

## Background request privacy

Trusted background callers can set `extra_body.__devo_background_request` to
`true`. The provider strips this internal marker before sending the request.
Background HTTP failure logs and returned errors exclude request/response
content; errors retain safe rate-limit, server, authentication, or invalid-request
classification through routing. Rate-limit errors retain numeric `Retry-After`
seconds. OpenAI streaming diagnostics also redact raw payloads and skip assistant
text previews for these requests. Foreground diagnostics keep their existing
behavior.
## What does not belong here

This crate is not responsible for:

- model catalog loading
- session management
- orchestration
- persistence
- UI-facing event shaping
- provider selection policy

Those concerns belong in higher-level crates such as `core`, `server`, and
`protocol`.

## Public exports

The crate root re-exports the provider traits and capability types:

- `devo_provider::ModelProviderSDK`
- `devo_provider::ProviderAdapter`
- `devo_provider::ProviderCapabilities`

It also exposes the provider family modules:

- `devo_provider::openai`
- `devo_provider::anthropic`

## Current scope

Today this crate ships:

- one Anthropic transport
- two OpenAI-family transports
- translation from provider wire formats into the shared protocol IR
- typed request/response parsing inside each adapter

If a new provider family or transport is added, it should follow the same
pattern:

1. map from `ModelRequest` into the provider wire format
2. map provider responses into `ModelResponse` and `StreamEvent`
3. expose provider-specific details through metadata when needed
4. report capabilities through `ProviderAdapter`
