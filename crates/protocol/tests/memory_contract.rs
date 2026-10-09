use std::collections::BTreeMap;

use devo_protocol::native::methods::NATIVE_METHODS;
use pretty_assertions::assert_eq;
use serde_json::{Value, json};

/// Trace: L2-DES-MEM-001 Rev 4 DD-12 / Verification Strategy.
/// Verifies: every Native memory method pins parameters, results, errors, capability and retry contract.
#[test]
fn every_native_memory_method_matches_pinned_contract() {
    let actual: BTreeMap<_, _> = NATIVE_METHODS
        .iter()
        .filter(|method| method.name.starts_with("memory/"))
        .map(|method| {
            (
                method.name,
                json!({
                    "params": (method.params_schema)(),
                    "result": (method.result_schema)(),
                    "errorCodes": method.error_codes,
                    "requiredCapability": method.required_capability,
                    "idempotency": format!("{:?}", method.idempotency),
                }),
            )
        })
        .collect();
    let expected: Value =
        serde_json::from_str(include_str!("golden/memory_contract.json")).unwrap();
    assert_eq!(serde_json::to_value(actual).unwrap(), expected);
}
