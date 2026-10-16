#![no_std]

use soroban_sdk::{contract, contracterror, contractimpl, contracttype, symbol_short, Address, Env, Vec, i128};

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Admin,
    OracleSources,
    MinSourcesRequired,
    PriceCache,
    PriceCacheTTL,
}

#[derive(Clone)]
#[contracttype]
pub struct PriceData {
    pub price: i128,
    pub decimals: u32,
    pub timestamp: u64,
    pub source: Address,
}

#[derive(Clone)]
#[contracttype]
pub struct AggregatedPrice {
    pub price: i128,
    pub decimals: u32,
    pub timestamp: u64,
    pub sources_used: u32,
    pub confidence: u32,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum OracleError {
    NotInitialized = 1,
    AlreadyInitialized = 2,
    InvalidAdmin = 3,
    InvalidOracleSource = 4,
    InsufficientSources = 5,
    StalePriceData = 6,
    InvalidPrice = 7,
    NoValidPrices = 8,
}

#[contract]
pub struct OracleAggregator;

#[contractimpl]
impl OracleAggregator {
    pub fn initialize(env: Env, admin: Address, min_sources: u32) -> Result<(), OracleError> {
        if env.storage().persistent().has(&DataKey::Admin) {
            return Err(OracleError::AlreadyInitialized);
        }

        admin.require_auth();

        env.storage().persistent().set(&DataKey::Admin, &admin);
        env.storage().persistent().set(&DataKey::MinSourcesRequired, &min_sources);
        env.storage().persistent().set(&DataKey::OracleSources, &Vec::<Address>::new(&env));
        env.storage().persistent().set(&DataKey::PriceCacheTTL, &3600u64);

        Ok(())
    }

    pub fn add_oracle_source(env: Env, oracle: Address) -> Result<(), OracleError> {
        let admin: Address = env
            .storage()
            .persistent()
            .get(&DataKey::Admin)
            .ok_or(OracleError::NotInitialized)?;

        admin.require_auth();

        let mut sources: Vec<Address> = env
            .storage()
            .persistent()
            .get(&DataKey::OracleSources)
            .unwrap_or_else(|| Vec::new(&env));

        if sources.contains(&oracle) {
            return Ok(());
        }

        sources.push_back(oracle);
        env.storage().persistent().set(&DataKey::OracleSources, &sources);

        Ok(())
    }

    pub fn remove_oracle_source(env: Env, oracle: Address) -> Result<(), OracleError> {
        let admin: Address = env
            .storage()
            .persistent()
            .get(&DataKey::Admin)
            .ok_or(OracleError::NotInitialized)?;

        admin.require_auth();

        let mut sources: Vec<Address> = env
            .storage()
            .persistent()
            .get(&DataKey::OracleSources)
            .ok_or(OracleError::NoValidPrices)?;

        sources.retain(|s| s != &oracle);
        env.storage().persistent().set(&DataKey::OracleSources, &sources);

        Ok(())
    }

    pub fn get_aggregated_price(
        env: Env,
        asset_id: Address,
    ) -> Result<AggregatedPrice, OracleError> {
        let sources: Vec<Address> = env
            .storage()
            .persistent()
            .get(&DataKey::OracleSources)
            .ok_or(OracleError::NoValidPrices)?;

        let min_sources: u32 = env
            .storage()
            .persistent()
            .get(&DataKey::MinSourcesRequired)
            .unwrap_or(1);

        let ttl: u64 = env
            .storage()
            .persistent()
            .get(&DataKey::PriceCacheTTL)
            .unwrap_or(3600);

        let current_time = env.ledger().timestamp();
        let mut prices: Vec<i128> = Vec::new(&env);
        let mut valid_sources = 0u32;

        for source in sources.iter() {
            match env.invoke_contract::<Address, (i128, u32, u64)>(
                source,
                &symbol_short!("price"),
                (asset_id.clone(), &env as &Env),
            ) {
                Ok((price, decimals, timestamp)) => {
                    if current_time.saturating_sub(timestamp) <= ttl && price > 0 {
                        prices.push_back(price);
                        valid_sources += 1;
                    }
                }
                Err(_) => continue,
            }
        }

        if valid_sources < min_sources {
            return Err(OracleError::InsufficientSources);
        }

        let aggregated = Self::median_price(&prices)?;

        Ok(AggregatedPrice {
            price: aggregated,
            decimals: 18,
            timestamp: current_time,
            sources_used: valid_sources,
            confidence: Self::calculate_confidence(valid_sources, sources.len()),
        })
    }

    fn median_price(prices: &Vec<i128>) -> Result<i128, OracleError> {
        if prices.is_empty() {
            return Err(OracleError::NoValidPrices);
        }

        let len = prices.len();
        let mut sorted = prices.clone();

        for i in 0..len {
            for j in (i + 1)..len {
                if sorted.get(j).unwrap() < sorted.get(i).unwrap() {
                    let temp = sorted.get(i).unwrap();
                    sorted.set(i, sorted.get(j).unwrap());
                    sorted.set(j, temp);
                }
            }
        }

        let median = if len % 2 == 0 {
            let mid = len / 2;
            (sorted.get(mid - 1).unwrap() + sorted.get(mid).unwrap()) / 2
        } else {
            sorted.get(len / 2).unwrap()
        };

        Ok(median)
    }

    fn calculate_confidence(valid: u32, total: u32) -> u32 {
        if total == 0 {
            return 0;
        }
        ((valid as u128 * 100) / (total as u128)) as u32
    }

    pub fn set_cache_ttl(env: Env, ttl_seconds: u64) -> Result<(), OracleError> {
        let admin: Address = env
            .storage()
            .persistent()
            .get(&DataKey::Admin)
            .ok_or(OracleError::NotInitialized)?;

        admin.require_auth();

        env.storage().persistent().set(&DataKey::PriceCacheTTL, &ttl_seconds);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use soroban_sdk::testutils::{Address as _, Env as _};

    #[test]
    fn test_initialize() {
        let env = Env::default();
        let admin = Address::random(&env);

        let result = OracleAggregator::initialize(env.clone(), admin.clone(), 2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_add_oracle_source() {
        let env = Env::default();
        let admin = Address::random(&env);
        let oracle = Address::random(&env);

        OracleAggregator::initialize(env.clone(), admin.clone(), 1).unwrap();
        let result = OracleAggregator::add_oracle_source(env, oracle);
        assert!(result.is_ok());
    }
}
