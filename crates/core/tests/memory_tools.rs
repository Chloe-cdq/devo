use devo_core::tools::create_default_tool_registry;
use devo_core::tools::is_subagent_agent_coordination_tool;

#[test]
fn default_registry_exposes_the_root_memory_remember_tool() {
    let registry = create_default_tool_registry();

    assert!(registry.get("memory_remember").is_some());
}

#[test]
fn memory_remember_is_not_available_to_subagents() {
    assert!(is_subagent_agent_coordination_tool("memory_remember"));
}
