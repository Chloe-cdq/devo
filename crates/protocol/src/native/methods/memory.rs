use super::{Idempotency, MethodSpec, schema_of};
use crate::native::rpc_memory::{
    MemoryEntry, MemoryExportParams, MemoryExportResult, MemoryForgetParams, MemoryForgetResult,
    MemoryListParams, MemoryListResult, MemoryRebuildParams, MemoryRebuildResult,
    MemoryRememberParams, MemoryResetParams, MemoryResetResult, MemoryStatus, MemoryStatusParams,
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

pub(super) const EXPORT: MethodSpec = MethodSpec {
    name: "memory/export",
    params_schema: schema_of::<MemoryExportParams>,
    result_schema: schema_of::<MemoryExportResult>,
    error_codes: &[],
    required_capability: None,
    idempotency: Idempotency::None,
};

pub(super) const RESET: MethodSpec = MethodSpec {
    name: "memory/reset",
    params_schema: schema_of::<MemoryResetParams>,
    result_schema: schema_of::<MemoryResetResult>,
    error_codes: &[],
    required_capability: None,
    idempotency: Idempotency::None,
};

pub(super) const REBUILD: MethodSpec = MethodSpec {
    name: "memory/rebuild",
    params_schema: schema_of::<MemoryRebuildParams>,
    result_schema: schema_of::<MemoryRebuildResult>,
    error_codes: &[],
    required_capability: None,
    idempotency: Idempotency::None,
};
