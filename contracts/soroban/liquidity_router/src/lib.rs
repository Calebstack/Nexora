#![no_std]

use soroban_sdk::{contract, contracterror, contractimpl, contracttype, symbol_short, token, Address, Env, Vec, i128};

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Admin,
    AMMs,
    PathCache,
    SlippageTolerance,
}

#[derive(Clone)]
#[contracttype]
pub struct AMMInfo {
    pub address: Address,
    pub token_a: Address,
    pub token_b: Address,
    pub fee: u32,
}

#[derive(Clone)]
#[contracttype]
pub struct SwapPath {
    pub amms: Vec<Address>,
    pub tokens: Vec<Address>,
    pub expected_output: i128,
}

#[derive(Clone)]
#[contracttype]
pub struct SwapResult {
    pub tokens_in: i128,
    pub tokens_out: i128,
    pub price_impact: u32,
    pub path_hops: u32,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum RouterError {
    NotInitialized = 1,
    InvalidAMM = 2,
    NoPath = 3,
    SlippageExceeded = 4,
    InsufficientLiquidity = 5,
    InvalidTokens = 6,
    AmountTooSmall = 7,
}

#[contract]
pub struct LiquidityRouter;

#[contractimpl]
impl LiquidityRouter {
    pub fn initialize(env: Env, admin: Address) -> Result<(), RouterError> {
        if env.storage().persistent().has(&DataKey::Admin) {
            return Err(RouterError::NotInitialized);
        }

        admin.require_auth();

        env.storage().persistent().set(&DataKey::Admin, &admin);
        env.storage().persistent().set(&DataKey::AMMs, &Vec::<AMMInfo>::new(&env));
        env.storage().persistent().set(&DataKey::SlippageTolerance, &50u32);

        Ok(())
    }

    pub fn register_amm(
        env: Env,
        amm_address: Address,
        token_a: Address,
        token_b: Address,
        fee: u32,
    ) -> Result<(), RouterError> {
        let admin: Address = env
            .storage()
            .persistent()
            .get(&DataKey::Admin)
            .ok_or(RouterError::NotInitialized)?;

        admin.require_auth();

        let mut amms: Vec<AMMInfo> = env
            .storage()
            .persistent()
            .get(&DataKey::AMMs)
            .unwrap_or_else(|| Vec::new(&env));

        amms.push_back(AMMInfo {
            address: amm_address,
            token_a,
            token_b,
            fee,
        });

        env.storage().persistent().set(&DataKey::AMMs, &amms);

        Ok(())
    }

    pub fn find_best_path(
        env: Env,
        token_in: Address,
        token_out: Address,
        amount_in: i128,
    ) -> Result<SwapPath, RouterError> {
        let amms: Vec<AMMInfo> = env
            .storage()
            .persistent()
            .get(&DataKey::AMMs)
            .ok_or(RouterError::NoPath)?;

        if token_in == token_out {
            return Err(RouterError::InvalidTokens);
        }

        let mut best_path = Vec::new(&env);
        let mut best_tokens = Vec::new(&env);
        let mut best_output: i128 = 0;

        for amm in amms.iter() {
            if (amm.token_a == token_in && amm.token_b == token_out)
                || (amm.token_b == token_in && amm.token_a == token_out)
            {
                best_path.push_back(amm.address.clone());
                best_tokens.push_back(token_in.clone());
                best_tokens.push_back(token_out.clone());
                best_output = Self::simulate_swap(&env, &amm.address, amount_in, amm.fee)?;
            }
        }

        if best_path.is_empty() {
            return Err(RouterError::NoPath);
        }

        Ok(SwapPath {
            amms: best_path,
            tokens: best_tokens,
            expected_output: best_output,
        })
    }

    pub fn execute_swap(
        env: Env,
        token_in: Address,
        token_out: Address,
        amount_in: i128,
        min_output: i128,
        recipient: Address,
    ) -> Result<SwapResult, RouterError> {
        let path = Self::find_best_path(env.clone(), token_in.clone(), token_out.clone(), amount_in)?;

        if path.expected_output < min_output {
            return Err(RouterError::SlippageExceeded);
        }

        let mut current_amount = amount_in;
        let mut hops = 0u32;

        for i in 0..path.amms.len() {
            let amm = path.amms.get(i).unwrap();

            current_amount = env
                .invoke_contract::<Address, i128>(
                    amm,
                    &symbol_short!("swap"),
                    (current_amount, &recipient as &Address),
                )
                .map_err(|_| RouterError::InsufficientLiquidity)?;

            hops += 1;
        }

        let price_impact = Self::calculate_price_impact(amount_in, current_amount);

        Ok(SwapResult {
            tokens_in: amount_in,
            tokens_out: current_amount,
            price_impact,
            path_hops: hops,
        })
    }

    fn simulate_swap(env: &Env, amm: &Address, amount_in: i128, fee: u32) -> Result<i128, RouterError> {
        let fee_amount = (amount_in as u128 * (fee as u128) / 10000u128) as i128;
        let amount_after_fee = amount_in - fee_amount;

        Ok(amount_after_fee)
    }

    fn calculate_price_impact(amount_in: i128, amount_out: i128) -> u32 {
        if amount_in == 0 {
            return 0;
        }

        let impact = (10000u128 - ((amount_out as u128 * 10000u128) / (amount_in as u128))) as u32;
        impact.min(10000)
    }

    pub fn set_slippage_tolerance(env: Env, tolerance_bps: u32) -> Result<(), RouterError> {
        let admin: Address = env
            .storage()
            .persistent()
            .get(&DataKey::Admin)
            .ok_or(RouterError::NotInitialized)?;

        admin.require_auth();

        env.storage().persistent().set(&DataKey::SlippageTolerance, &tolerance_bps);

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

        let result = LiquidityRouter::initialize(env, admin);
        assert!(result.is_ok());
    }
}
