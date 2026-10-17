use soroban_sdk::Env;

pub enum StorageType {
    Persistent,
    Temporary,
    Instance,
}

pub struct StorageOptimizer;

impl StorageOptimizer {
    pub fn get_storage_type(key_importance: u8) -> StorageType {
        match key_importance {
            0..=2 => StorageType::Temporary,
            3..=5 => StorageType::Instance,
            _ => StorageType::Persistent,
        }
    }

    pub fn estimate_footprint(data_size: usize) -> u32 {
        ((data_size + 31) / 32) as u32
    }

    pub fn extend_storage_ttl(env: &Env, extension_ledgers: u64) {
        let ttl = env.storage().persistent().extend_ttl(extension_ledgers, extension_ledgers);
    }
}

pub struct TTLConfig {
    pub safety_threshold: u64,
    pub max_renewal_cost: i128,
    pub extension_period: u64,
}

impl Default for TTLConfig {
    fn default() -> Self {
        TTLConfig {
            safety_threshold: 50_000,
            max_renewal_cost: 10_000_000,
            extension_period: 500_000,
        }
    }
}
