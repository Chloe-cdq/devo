use super::{Idempotency, MethodSpec, schema_of};
use crate::native::rpc_memory::{
    MemoryEntry, MemoryForgetParams, MemoryForgetResult, MemoryListParams, MemoryListResult,
    MemoryRememberParams, MemoryStatus, MemoryStatusParams,
};

pub(super) const STATUS: MethodSpec = MethodSpec {
    name: "memory/status",
    params_schema: schema_of::<MemoryStatusParams>,
    result_schema: schema_of::<MemoryStatus>,
    error_codes: &[],
    required_capability: None,
    idempotency: Idempotency::None,
};

pub(super) const REMEMBER: MethodSpec = MethodSpec {
    name: "memory/remember",
    params_schema: schema_of::<MemoryRememberParams>,
    result_schema: schema_of::<MemoryEntry>,
    error_codes: &[],
    required_capability: None,
    idempotency: Idempotency::None,
};

pub(super) const FORGET: MethodSpec = MethodSpec {
    name: "memory/forget",
    params_schema: schema_of::<MemoryForgetParams>,
    result_schema: schema_of::<MemoryForgetResult>,
    error_codes: &[],
    required_capability: None,
    idempotency: Idempotency::None,
};

pub(super) const LIST: MethodSpec = MethodSpec {
    name: "memory/list",
    params_schema: schema_of::<MemoryListParams>,
    result_schema: schema_of::<MemoryListResult>,
    error_codes: &[],
    required_capability: None,
    idempotency: Idempotency::None,
};
