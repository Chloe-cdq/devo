use devo_protocol::native::methods::NATIVE_METHODS;
use devo_protocol::native::rpc_memory::{
    MemoryExportParams, MemoryExportResult, MemoryResetParams,
};
use pretty_assertions::assert_eq;
use serde_json::json;

/// Trace: L2-DES-MEM-001 DD-9, DD-10. Native clients must explicitly choose the reset scope.
#[test]
fn scoped_management_schemas_and_deserializers_require_scope() {
    for name in ["memory/export", "memory/reset"] {
        let method = NATIVE_METHODS
            .iter()
            .find(|method| method.name == name)
            .expect("registered Native method");
        let schema = serde_json::to_value((method.params_schema)()).unwrap();
        assert_eq!(schema["required"], json!(["scope"]));
    }
    for value in [json!({}), json!({"scope": null}), json!({"scope": "all"})] {
        assert!(serde_json::from_value::<MemoryExportParams>(value.clone()).is_err());
        assert!(serde_json::from_value::<MemoryResetParams>(value).is_err());
    }
}

/// Trace: L2-DES-MEM-001 DD-12. Portable exports keep lifecycle fences in Native wire format.
#[test]
fn export_bundle_roundtrips_lifecycle_metadata() {
    let wire = json!({"scope": "project", "markdown": "# Project Memory\n", "lifecycle": {
        "ignoreSourcesBefore": "2026-10-08T00:00:00Z", "lastRebuildAt": null, "revocationCount": 2
    }});
    let result: MemoryExportResult = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(result).unwrap(), wire);
}
