#![no_std]

use soroban_sdk::{
    contract, contracterror, contractimpl, contracttype, symbol_short, token, Address, BytesN, Env,
};

const BPS: i128 = 10_000;

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Admin,
    Pool(BytesN<32>),
    Shares(BytesN<32>, Address),
}

/// Soroban-side mirror of a Stellar Classic constant-product pool. Assets are
/// the Stellar Asset Contract addresses of the pool's two classic assets.
/// `total_shares` is the supply of the wrapped pool share token (PST).
#[derive(Clone, Debug, Eq, PartialEq)]
#[contracttype]
pub struct Pool {
    pub asset_a: Address,
    pub asset_b: Address,
    pub reserve_a: i128,
    pub reserve_b: i128,
    pub total_shares: i128,
    pub fee_bps: u32,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum BridgeError {
    NotInitialized = 1,
    AlreadyInitialized = 2,
    PoolNotFound = 3,
    PoolExists = 4,
    InvalidAmount = 5,
    InvalidAsset = 6,
    SlippageExceeded = 7,
    ReservesImbalance = 8,
    InsufficientShares = 9,
    InsufficientLiquidity = 10,
    InvariantViolated = 11,
    Overflow = 12,
}

#[contract]
pub struct ClassicBridge;

#[contractimpl]
impl ClassicBridge {
    pub fn initialize(env: Env, admin: Address) -> Result<(), BridgeError> {
        if env.storage().instance().has(&DataKey::Admin) {
            return Err(BridgeError::AlreadyInitialized);
        }
        admin.require_auth();
        env.storage().instance().set(&DataKey::Admin, &admin);
        Ok(())
    }

    pub fn register_pool(
        env: Env,
        pool_id: BytesN<32>,
        asset_a: Address,
        asset_b: Address,
        fee_bps: u32,
    ) -> Result<(), BridgeError> {
        Self::admin(&env)?.require_auth();
        if env.storage().persistent().has(&DataKey::Pool(pool_id.clone())) {
            return Err(BridgeError::PoolExists);
        }
        if asset_a == asset_b || fee_bps as i128 >= BPS {
            return Err(BridgeError::InvalidAsset);
        }
        let pool = Pool {
            asset_a,
            asset_b,
            reserve_a: 0,
            reserve_b: 0,
            total_shares: 0,
            fee_bps,
        };
        env.storage().persistent().set(&DataKey::Pool(pool_id), &pool);
        Ok(())
    }

    /// Called by the Go bridge once a classic pool transaction is confirmed,
    /// so the on-chain mirror follows the classic pool's real reserves.
    pub fn sync_reserves(
        env: Env,
        pool_id: BytesN<32>,
        reserve_a: i128,
        reserve_b: i128,
        total_shares: i128,
    ) -> Result<(), BridgeError> {
        Self::admin(&env)?.require_auth();
        if reserve_a < 0 || reserve_b < 0 || total_shares < 0 {
            return Err(BridgeError::InvalidAmount);
        }
        let mut pool = Self::pool(&env, &pool_id)?;
        pool.reserve_a = reserve_a;
        pool.reserve_b = reserve_b;
        pool.total_shares = total_shares;
        Self::save(&env, &pool_id, &pool);
        env.events()
            .publish((symbol_short!("sync"), pool_id), (reserve_a, reserve_b, total_shares));
        Ok(())
    }

    /// Deposits both assets and wraps the resulting pool shares. The pool
    /// price must not move: the deposit has to match the reserve ratio
    /// within the caller's `min_shares` tolerance, and any excess of the
    /// less-valuable side is not taken.
    pub fn deposit(
        env: Env,
        pool_id: BytesN<32>,
        depositor: Address,
        amount_a: i128,
        amount_b: i128,
        min_shares: i128,
    ) -> Result<i128, BridgeError> {
        depositor.require_auth();
        if amount_a <= 0 || amount_b <= 0 {
            return Err(BridgeError::InvalidAmount);
        }
        let mut pool = Self::pool(&env, &pool_id)?;

        let (used_a, used_b, shares) = if pool.total_shares == 0 {
            let shares = Self::isqrt(Self::mul(amount_a, amount_b)?);
            (amount_a, amount_b, shares)
        } else {
            if pool.reserve_a == 0 || pool.reserve_b == 0 {
                return Err(BridgeError::ReservesImbalance);
            }
            let shares_a = Self::mul(amount_a, pool.total_shares)? / pool.reserve_a;
            let shares_b = Self::mul(amount_b, pool.total_shares)? / pool.reserve_b;
            let shares = shares_a.min(shares_b);
            // Take only the amounts that correspond to the minted shares.
            let used_a = Self::ceil_div(Self::mul(shares, pool.reserve_a)?, pool.total_shares);
            let used_b = Self::ceil_div(Self::mul(shares, pool.reserve_b)?, pool.total_shares);
            (used_a, used_b, shares)
        };

        if shares <= 0 {
            return Err(BridgeError::InvalidAmount);
        }
        if shares < min_shares {
            return Err(BridgeError::SlippageExceeded);
        }

        let me = env.current_contract_address();
        token::Client::new(&env, &pool.asset_a).transfer(&depositor, &me, &used_a);
        token::Client::new(&env, &pool.asset_b).transfer(&depositor, &me, &used_b);

        pool.reserve_a = pool.reserve_a.checked_add(used_a).ok_or(BridgeError::Overflow)?;
        pool.reserve_b = pool.reserve_b.checked_add(used_b).ok_or(BridgeError::Overflow)?;
        pool.total_shares = pool.total_shares.checked_add(shares).ok_or(BridgeError::Overflow)?;
        Self::save(&env, &pool_id, &pool);

        let key = DataKey::Shares(pool_id.clone(), depositor.clone());
        let held: i128 = env.storage().persistent().get(&key).unwrap_or(0);
        env.storage().persistent().set(&key, &(held + shares));

        env.events()
            .publish((symbol_short!("deposit"), pool_id), (depositor, used_a, used_b, shares));
        Ok(shares)
    }

    /// Burns wrapped pool shares and redeems the pro-rata SAC tokens.
    pub fn withdraw(
        env: Env,
        pool_id: BytesN<32>,
        owner: Address,
        shares: i128,
        min_a: i128,
        min_b: i128,
    ) -> Result<(i128, i128), BridgeError> {
        owner.require_auth();
        if shares <= 0 {
            return Err(BridgeError::InvalidAmount);
        }
        let mut pool = Self::pool(&env, &pool_id)?;

        let key = DataKey::Shares(pool_id.clone(), owner.clone());
        let held: i128 = env.storage().persistent().get(&key).unwrap_or(0);
        if held < shares {
            return Err(BridgeError::InsufficientShares);
        }

        let out_a = Self::mul(shares, pool.reserve_a)? / pool.total_shares;
        let out_b = Self::mul(shares, pool.reserve_b)? / pool.total_shares;
        if out_a < min_a || out_b < min_b {
            return Err(BridgeError::SlippageExceeded);
        }

        pool.reserve_a -= out_a;
        pool.reserve_b -= out_b;
        pool.total_shares -= shares;
        Self::save(&env, &pool_id, &pool);
        env.storage().persistent().set(&key, &(held - shares));

        let me = env.current_contract_address();
        token::Client::new(&env, &pool.asset_a).transfer(&me, &owner, &out_a);
        token::Client::new(&env, &pool.asset_b).transfer(&me, &owner, &out_b);

        env.events()
            .publish((symbol_short!("withdraw"), pool_id), (owner, out_a, out_b, shares));
        Ok((out_a, out_b))
    }

    /// Swaps `amount_in` of `token_in` through the pool at the constant
    /// product price, then re-checks that the product never decreased.
    pub fn swap(
        env: Env,
        pool_id: BytesN<32>,
        trader: Address,
        token_in: Address,
        amount_in: i128,
        min_out: i128,
    ) -> Result<i128, BridgeError> {
        trader.require_auth();
        let mut pool = Self::pool(&env, &pool_id)?;
        let a_to_b = Self::direction(&pool, &token_in)?;
        let (reserve_in, reserve_out) = if a_to_b {
            (pool.reserve_a, pool.reserve_b)
        } else {
            (pool.reserve_b, pool.reserve_a)
        };

        let amount_out = Self::quote_out(reserve_in, reserve_out, amount_in, pool.fee_bps)?;
        if amount_out < min_out {
            return Err(BridgeError::SlippageExceeded);
        }

        let new_in = reserve_in.checked_add(amount_in).ok_or(BridgeError::Overflow)?;
        let new_out = reserve_out - amount_out;
        if Self::mul(new_in, new_out)? < Self::mul(reserve_in, reserve_out)? {
            return Err(BridgeError::InvariantViolated);
        }

        let (asset_in, asset_out) = if a_to_b {
            (pool.asset_a.clone(), pool.asset_b.clone())
        } else {
            (pool.asset_b.clone(), pool.asset_a.clone())
        };
        if a_to_b {
            pool.reserve_a = new_in;
            pool.reserve_b = new_out;
        } else {
            pool.reserve_b = new_in;
            pool.reserve_a = new_out;
        }
        Self::save(&env, &pool_id, &pool);

        let me = env.current_contract_address();
        token::Client::new(&env, &asset_in).transfer(&trader, &me, &amount_in);
        token::Client::new(&env, &asset_out).transfer(&me, &trader, &amount_out);

        env.events()
            .publish((symbol_short!("swap"), pool_id), (trader, amount_in, amount_out));
        Ok(amount_out)
    }

    /// Exact output for `amount_in` of `token_in`, for off-chain callers.
    pub fn quote(
        env: Env,
        pool_id: BytesN<32>,
        token_in: Address,
        amount_in: i128,
    ) -> Result<i128, BridgeError> {
        let pool = Self::pool(&env, &pool_id)?;
        let (reserve_in, reserve_out) = if Self::direction(&pool, &token_in)? {
            (pool.reserve_a, pool.reserve_b)
        } else {
            (pool.reserve_b, pool.reserve_a)
        };
        Self::quote_out(reserve_in, reserve_out, amount_in, pool.fee_bps)
    }

    /// True when a classic trade of `amount_in` for `amount_out` is exactly
    /// what the constant-product formula allows: not more output than the
    /// formula gives, and the product does not shrink.
    pub fn verify_trade(
        env: Env,
        pool_id: BytesN<32>,
        token_in: Address,
        amount_in: i128,
        amount_out: i128,
    ) -> Result<bool, BridgeError> {
        let expected = Self::quote(env, pool_id, token_in, amount_in)?;
        Ok(amount_out > 0 && amount_out <= expected)
    }

    pub fn get_pool(env: Env, pool_id: BytesN<32>) -> Result<Pool, BridgeError> {
        Self::pool(&env, &pool_id)
    }

    pub fn shares_of(env: Env, pool_id: BytesN<32>, owner: Address) -> i128 {
        env.storage()
            .persistent()
            .get(&DataKey::Shares(pool_id, owner))
            .unwrap_or(0)
    }

    fn admin(env: &Env) -> Result<Address, BridgeError> {
        env.storage()
            .instance()
            .get(&DataKey::Admin)
            .ok_or(BridgeError::NotInitialized)
    }

    fn pool(env: &Env, pool_id: &BytesN<32>) -> Result<Pool, BridgeError> {
        env.storage()
            .persistent()
            .get(&DataKey::Pool(pool_id.clone()))
            .ok_or(BridgeError::PoolNotFound)
    }

    fn save(env: &Env, pool_id: &BytesN<32>, pool: &Pool) {
        env.storage().persistent().set(&DataKey::Pool(pool_id.clone()), pool);
    }

    /// true when `token_in` is asset A.
    fn direction(pool: &Pool, token_in: &Address) -> Result<bool, BridgeError> {
        if *token_in == pool.asset_a {
            Ok(true)
        } else if *token_in == pool.asset_b {
            Ok(false)
        } else {
            Err(BridgeError::InvalidAsset)
        }
    }

    fn quote_out(
        reserve_in: i128,
        reserve_out: i128,
        amount_in: i128,
        fee_bps: u32,
    ) -> Result<i128, BridgeError> {
        if amount_in <= 0 {
            return Err(BridgeError::InvalidAmount);
        }
        if reserve_in <= 0 || reserve_out <= 0 {
            return Err(BridgeError::InsufficientLiquidity);
        }
        let in_after_fee = Self::mul(amount_in, BPS - fee_bps as i128)?;
        let numerator = Self::mul(in_after_fee, reserve_out)?;
        let denominator = Self::mul(reserve_in, BPS)?
            .checked_add(in_after_fee)
            .ok_or(BridgeError::Overflow)?;
        let out = numerator / denominator;
        if out <= 0 || out >= reserve_out {
            return Err(BridgeError::InsufficientLiquidity);
        }
        Ok(out)
    }

    fn mul(a: i128, b: i128) -> Result<i128, BridgeError> {
        a.checked_mul(b).ok_or(BridgeError::Overflow)
    }

    fn ceil_div(a: i128, b: i128) -> i128 {
        (a + b - 1) / b
    }

    fn isqrt(n: i128) -> i128 {
        if n < 2 {
            return n;
        }
        let mut x = n;
        let mut y = (x + 1) / 2;
        while y < x {
            x = y;
            y = (x + n / x) / 2;
        }
        x
    }
}

#[cfg(test)]
mod test;
