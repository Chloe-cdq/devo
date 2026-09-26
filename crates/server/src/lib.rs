//! Server runtime and protocol contracts.
//!
//! Trace: L2-DES-MEM-001 Rev 4 DD-13
//! Verifies: memory command execution remains an internal implementation seam.
//!
//! ```compile_fail
//! use devo_server::MemoryCommandExecutor;
//! ```

#[cfg(test)]
extern crate self as devo_server;

mod approval;
mod approval_reviewer;
mod bootstrap;
mod client;
mod connection;
pub mod db;
mod event;
mod event_reconcile;
mod exec_policy_store;
mod execution;
pub mod goal;
mod goal_durable;
pub mod memory;
mod persistence;
mod projection;
mod protocol;
mod protocols;
mod provider_config;
mod runtime;
mod sandbox_profile;
mod session;
mod session_context;
mod singleton;
pub mod subagent;
mod titles;
mod tool_actions;
mod transport;
mod turn;
mod usage_ledger;
mod workspace_changes;

#[cfg(test)]
#[path = "../tests/support/memory_forget_cases/memory_forget_agent_cancellation.rs"]
mod memory_forget_agent_cancellation;
#[cfg(test)]
#[path = "../tests/support/memory_forget_cases/memory_forget_concurrency.rs"]
mod memory_forget_concurrency;
#[cfg(test)]
#[path = "../tests/support/memory_forget_cases/memory_forget_durability.rs"]
mod memory_forget_durability;
#[cfg(test)]
#[path = "../tests/support/memory_forget_cases/memory_forget_native.rs"]
mod memory_forget_native;
#[cfg(test)]
#[path = "../tests/support/memory_forget_cases/memory_forget_project_preflight.rs"]
mod memory_forget_project_preflight;
#[cfg(test)]
#[path = "../tests/support/memory_forget_runtime.rs"]
mod memory_forget_runtime_support;
#[cfg(test)]
#[path = "runtime/memory_forget_test_support.rs"]
mod memory_forget_support;
#[cfg(test)]
#[path = "../tests/support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;

pub use approval::*;
pub use bootstrap::*;
pub use client::*;
pub use connection::*;
pub use event::*;
pub use execution::ServerRuntimeDependencies;
pub use execution::empty_mcp_manager;
pub use projection::*;
pub use protocol::*;
pub use protocols::*;
pub use provider_config::*;
pub use runtime::*;
pub use session::*;
pub use transport::*;
pub use turn::*;
