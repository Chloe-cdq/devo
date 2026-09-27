//! Server runtime and protocol contracts.

#[cfg(test)]
extern crate self as devo_server;

/// Trace: L2-DES-MEM-001 Rev 4 DD-13
/// Verifies: memory command execution remains an internal implementation seam.
///
/// ```compile_fail
/// use devo_server::MemoryCommandExecutor;
/// ```
#[allow(dead_code)]
fn memory_command_executor_is_internal() {}

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
include!("memory_forget_tests.rs");

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
