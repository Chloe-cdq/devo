use devo_core::tools::{create_default_tool_registry, is_subagent_agent_coordination_tool};

/// Trace: L2-DES-MEM-001 Rev 4 Built-in Agent Tools
/// Verifies: the root registry exposes on-demand stable-ID reads.
#[test]
fn root_registry_exposes_memory_read() {
    assert!(create_default_tool_registry().get("memory_read").is_some());
}

/// Trace: L2-DES-MEM-001 Rev 4 Built-in Agent Tools
/// Verifies: read tools and aliases remain hidden from delegated agents.
#[test]
fn subagents_cannot_discover_memory_read() {
    for name in ["memory_read", "memory-read"] {
        assert!(is_subagent_agent_coordination_tool(name));
    }
}
