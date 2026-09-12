use devo_core::tools::create_default_tool_registry;
use devo_core::tools::is_subagent_agent_coordination_tool;

/// Trace: L2-DES-MEM-001
/// Verifies: the default registry exposes the root memory remember action.
#[test]
fn default_registry_exposes_the_root_memory_remember_tool() {
    let registry = create_default_tool_registry();

    assert!(registry.get("memory_remember").is_some());
}

/// Trace: L2-DES-MEM-001 DD-12
/// Verifies: the default registry exposes the root memory forget action.
#[test]
fn default_registry_exposes_the_root_memory_forget_tool() {
    let registry = create_default_tool_registry();

    assert!(registry.get("memory_forget").is_some());
}

/// Trace: L2-DES-MEM-001
/// Verifies: the root agent can search memory before selecting a stable ID.
#[test]
fn default_registry_exposes_the_root_memory_search_tool() {
    let registry = create_default_tool_registry();

    assert!(registry.get("memory_search").is_some());
}

/// Trace: L2-DES-MEM-001
/// Verifies: subagents cannot receive the mutating memory remember action.
#[test]
fn memory_remember_is_not_available_to_subagents() {
    assert!(is_subagent_agent_coordination_tool("memory_remember"));
}

/// Trace: L2-DES-MEM-001 DD-12
/// Verifies: subagents cannot receive the mutating memory forget action.
#[test]
fn memory_forget_is_not_available_to_subagents() {
    assert!(is_subagent_agent_coordination_tool("memory_forget"));
}

/// Trace: L2-DES-MEM-001
/// Verifies: subagents cannot independently search user memory.
#[test]
fn memory_search_is_not_available_to_subagents() {
    assert!(is_subagent_agent_coordination_tool("memory_search"));
}
