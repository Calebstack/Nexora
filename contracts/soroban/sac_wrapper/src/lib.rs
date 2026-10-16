#![no_std]

use soroban_sdk::{contract, contracterror, contractimpl, contracttype, symbol_short, token, Address, Env, Bytes, Vec, i128, u64};

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    SACAddress,
    Nonce,
    PermitNonce,
    AllowedSpenders,
}

#[derive(Clone)]
#[contracttype]
pub struct Permit {
    pub spender: Address,
    pub amount: i128,
    pub nonce: u64,
    pub expiration: u64,
    pub signature: Bytes,
}

#[derive(Clone)]
#[contracttype]
pub struct AllowanceInfo {
    pub spender: Address,
    pub amount: i128,
    pub expiration: u64,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum SACError {
    NotInitialized = 1,
    AlreadyInitialized = 2,
    InvalidSignature = 3,
    ExpiredPermit = 4,
    InsufficientAllowance = 5,
    InvalidAmount = 6,
    NonceAlreadyUsed = 7,
}

#[contract]
pub struct SACWrapper;

#[contractimpl]
impl SACWrapper {
    pub fn initialize(env: Env, sac_address: Address) -> Result<(), SACError> {
        if env.storage().persistent().has(&DataKey::SACAddress) {
            return Err(SACError::AlreadyInitialized);
        }

        env.storage().persistent().set(&DataKey::SACAddress, &sac_address);
        env.storage().persistent().set(&DataKey::PermitNonce, &0u64);

        Ok(())
    }

    pub fn permit(
        env: Env,
        owner: Address,
        permit: Permit,
    ) -> Result<(), SACError> {
        let current_time = env.ledger().timestamp();

        if current_time > permit.expiration {
            return Err(SACError::ExpiredPermit);
        }

        let permit_nonce: u64 = env
            .storage()
            .persistent()
            .get(&DataKey::PermitNonce)
            .unwrap_or(0);

        if permit.nonce <= permit_nonce {
            return Err(SACError::NonceAlreadyUsed);
        }

        Self::verify_signature(&env, &owner, &permit)?;

        let sac: Address = env
            .storage()
            .persistent()
            .get(&DataKey::SACAddress)
            .ok_or(SACError::NotInitialized)?;

        env.invoke_contract::<Address, ()>(
            &sac,
            &symbol_short!("approve"),
            (owner, permit.spender, permit.amount, permit.expiration),
        );

        env.storage()
            .persistent()
            .set(&DataKey::PermitNonce, &permit.nonce);

        Ok(())
    }

    pub fn transfer_from_with_permit(
        env: Env,
        owner: Address,
        spender: Address,
        amount: i128,
        permit: Permit,
    ) -> Result<(), SACError> {
        if amount <= 0 {
            return Err(SACError::InvalidAmount);
        }

        Self::permit(env.clone(), owner.clone(), permit)?;

        let sac: Address = env
            .storage()
            .persistent()
            .get(&DataKey::SACAddress)
            .ok_or(SACError::NotInitialized)?;

        env.invoke_contract::<Address, bool>(
            &sac,
            &symbol_short!("transfer_from"),
            (owner, spender, amount),
        )
        .map_err(|_| SACError::InsufficientAllowance)?;

        Ok(())
    }

    pub fn transfer(
        env: Env,
        from: Address,
        to: Address,
        amount: i128,
    ) -> Result<bool, SACError> {
        if amount <= 0 {
            return Err(SACError::InvalidAmount);
        }

        let sac: Address = env
            .storage()
            .persistent()
            .get(&DataKey::SACAddress)
            .ok_or(SACError::NotInitialized)?;

        from.require_auth();

        env.invoke_contract::<Address, bool>(
            &sac,
            &symbol_short!("transfer"),
            (from, to, amount),
        )
        .map_err(|_| SACError::InvalidAmount)
    }

    pub fn balance(env: Env, account: Address) -> Result<i128, SACError> {
        let sac: Address = env
            .storage()
            .persistent()
            .get(&DataKey::SACAddress)
            .ok_or(SACError::NotInitialized)?;

        env.invoke_contract::<Address, i128>(
            &sac,
            &symbol_short!("balance"),
            account,
        )
        .map_err(|_| SACError::NotInitialized)
    }

    pub fn approve(
        env: Env,
        owner: Address,
        spender: Address,
        amount: i128,
        expiration: u64,
    ) -> Result<(), SACError> {
        if amount < 0 {
            return Err(SACError::InvalidAmount);
        }

        owner.require_auth();

        let sac: Address = env
            .storage()
            .persistent()
            .get(&DataKey::SACAddress)
            .ok_or(SACError::NotInitialized)?;

        env.invoke_contract::<Address, ()>(
            &sac,
            &symbol_short!("approve"),
            (owner, spender, amount, expiration),
        );

        Ok(())
    }

    fn verify_signature(env: &Env, owner: &Address, permit: &Permit) -> Result<(), SACError> {
        let message_hash = env.crypto().sha256(&Bytes::new(env));

        env.crypto()
            .ed25519_verify(&owner.try_into().map_err(|_| SACError::InvalidSignature)?, &message_hash, &permit.signature);

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
        let sac = Address::random(&env);

        let result = SACWrapper::initialize(env, sac);
        assert!(result.is_ok());
    }
}
