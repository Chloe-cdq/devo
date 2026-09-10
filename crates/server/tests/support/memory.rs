use uuid::Uuid;

pub fn deterministic_uuid(seed: &str) -> Uuid {
    let value = seed.bytes().fold(0_u128, |value, byte| {
        value.rotate_left(5) ^ u128::from(byte)
    });
    Uuid::from_u128(value)
}
