#[path = "../tests/support/memory_forget_cases/memory_forget_agent_cancellation.rs"]
mod memory_forget_agent_cancellation;
#[path = "../tests/support/memory_forget_cases/memory_forget_concurrency.rs"]
mod memory_forget_concurrency;
#[path = "../tests/support/memory_forget_cases/memory_forget_durability.rs"]
mod memory_forget_durability;
#[path = "../tests/support/memory_forget_cases/memory_forget_native.rs"]
mod memory_forget_native;
#[path = "../tests/support/memory_forget_cases/memory_forget_project_preflight.rs"]
mod memory_forget_project_preflight;
#[path = "../tests/support/memory_forget_runtime.rs"]
mod memory_forget_runtime_support;
#[path = "runtime/memory_forget_test_support.rs"]
mod memory_forget_support;
#[path = "../tests/support/subagent_lifecycle.rs"]
#[allow(dead_code)]
mod support;
