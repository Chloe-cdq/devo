---
artifact_id: L1-REQ-APP-012
revision: 1
status: Draft
active_baseline: no
supersedes:
superseded_by:
owner: Human
last_updated: 2026-08-25
---

# L1-REQ-APP-012 — Privacy and Data Ownership

## Purpose

Make ownership and movement of user data explicit.

## Why This Matters

The program handles project files, conversation history, logs, credentials, and external service calls. Users need to know what stays local, what may leave the machine, and how they can control stored data.

## Background / Context

The program handles conversation history, core-maintained persistent memory, file contents, tool output, credentials, model prompts, logs, and optional telemetry. Users need clear control over user-visible stored data and external data sharing boundaries.

Credential data may need to pass through a client interface when the user is explicitly configuring or managing provider access. The program should distinguish those explicit credential flows from ordinary model selection, provider status, transcript, logging, and model-context flows.

Provider custom HTTP headers are an explicit raw configuration path. Users may store secret-bearing header values in configuration when they choose this path, and the program should treat those values as sensitive local configuration data rather than ordinary status, transcript, log, telemetry, or model-context data.

## User / Business Requirement

The program must protect user data, explain external data sharing, and provide control over stored data.

## Real User Scenarios

- A user asks whether a task will send file content to a model provider before approving the work.
- A user enters an API key during provider setup and expects ordinary status views to show whether it is configured without displaying the plaintext key by default.
- A user wants to delete a session and expects stored history and cached artifacts for that session to be removed or clearly retained by policy.
- A user deletes a session and expects the program to remove or retain user-visible session data according to the deletion policy without requiring manual persistent-memory management.

## Functional Requirements

- The program must treat session history, core-maintained persistent memory when model-visible, file contents, tool output, configuration, and local cache as user data.
- The program must make external data sharing boundaries visible for model providers, MCP servers, web services, and telemetry services.
- The program must prevent secrets and credentials from being exposed as ordinary model context.
- The program may allow client interfaces to handle credential material during explicit user-initiated credential setup, update, repair, or user-authorized reveal flows.
- The program may allow secret-bearing custom provider HTTP headers to be configured as explicit raw configuration values.
- The program must avoid exposing plaintext credential values in routine client views such as model lists, model switchers, provider status displays, transcripts, logs, or telemetry by default.
- The program must avoid exposing custom provider HTTP header values in routine client views, transcripts, logs, or telemetry by default.
- The user must be able to export and delete persistent user data.
- General Persistent Memory is maintained by the server runtime; clients use the canonical Native management surface rather than owning memory state.
- Users must be able to inspect safe entry projections and provenance, forget, export, and reset General Persistent Memory. Individual first-party clients may expose these controls progressively, and ordinary transcript views need not render the full memory store.
- When the user deletes a session, the server applies the deterministic evidence policy without requiring per-memory decisions; an optional related-memory deletion remains an explicit user choice.

## Non-Functional Requirements

- Telemetry must be user-controllable.
- Logs and tool output must not intentionally preserve plaintext secrets.

## Acceptance Criteria

- Given telemetry is disabled, when the program runs, then telemetry data is not sent.
- Given stored session history, when the user requests deletion, then the program removes it or reports why it cannot.
- Given content is sent to an external provider, when the user reviews privacy-relevant state, then the program can identify the type of data involved.
- Given a user provides credential material through an explicit setup or update flow, when the program receives it, then that flow is treated as credential handling rather than ordinary transcript, model context, logging, or telemetry data.
- Given a routine client view needs to show provider or model credential state, when the view is rendered, then it uses status information rather than plaintext credential values by default.
- Given the user configures a custom provider HTTP header value, when routine client views, logs, transcripts, or telemetry are produced, then the plaintext header value is not exposed by default.
- Given a secret is detected in tool output, when the output is recorded, then plaintext secret exposure is avoided where the safety policy requires it.
- Given General Persistent Memory is enabled, when a client renders an ordinary session, then it may show the bounded recall event without exposing raw memory-store records or source transcripts.
- Given persistent memory contributes to model-visible context, when safety or privacy controls are applied, then it is treated as model-visible user data.

## Out of Scope

- The program does not define secret-detection rules, credential-store backend, or telemetry protocol in this L1 requirement.
- The program does not define exact credential reveal, rotation, masking, or redaction controls in this L1 requirement.
- The program does not define persistent memory ranking, retrieval, summarization, extraction, retention, or internal deletion algorithms in this L1 requirement.
- This requirement does not claim that all sensitive data can be detected perfectly.

## Open Questions

- Should telemetry default to disabled or require an onboarding decision?
- None for General Persistent Memory; L1-REQ-MEM-001 defines the approved inspection and control requirement.

## Traceability

| Relationship | Target ID | Target Revision | Target Path | Rationale |
|---|---|---:|---|---|
| related-to | L1-REQ-MEM-001 | 2 | specs/L1/L1-REQ-MEM-001-persistent-memory.md | General Persistent Memory is server-owned user data with approved inspection, export, forget, and reset controls. |
| related-to | L2-DES-APP-003 | 2 | specs/L2/app/L2-DES-APP-003-client-server-protocol.md | Native exposes management while ordinary clients and transcript views remain decoupled from store internals. |
| related-to | L2-DES-APP-005 | 2 | specs/L2/app/L2-DES-APP-005-config-toml-schema.md | L2 defines the explicit custom provider header configuration exception and safe handling rules. |

## Revision Notes

| Revision | Date | Author | Change Type | Notes |
|---:|---|---|---|---|
| 1 | 2026-05-20 | Assistant | Initial | Initial draft with approved L1 refinement. |
| 1 | 2026-05-22 | Human | Refinement | Added explicit credential-flow requirements and clarified that routine client, transcript, logging, telemetry, and model-context paths should not expose plaintext credentials by default. |
| 1 | 2026-05-22 | Human | Refinement | Added persistent memory ownership and session-deletion impact requirements. |
| 1 | 2026-05-22 | Human | Refinement | Reframed persistent memory as core-maintained state outside routine client management. |
| 1 | 2026-06-08 | Human | Refinement | Added explicit raw provider HTTP header configuration as a sensitive configuration path. |
| 1 | 2026-08-25 | Human + Assistant | Refinement | Aligned memory data ownership with the approved Native inspection, export, forget, reset, and bounded recall-visibility model. |
