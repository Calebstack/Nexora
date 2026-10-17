#![no_std]

use soroban_sdk::{
    contract, contracterror, contractimpl, contracttype, symbol_short, token, Address, Env,
    Vec,
};

const HUNDRED_PERCENT_BPS: u32 = 10_000;

#[derive(Clone)]
#[contracttype]
pub struct Beneficiary {
    pub address: Address,
    pub basis_points: u32,
}

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Asset,
    Admin,
    Beneficiaries,
    PrimaryRecipient,
}

#[derive(Clone)]
#[contracttype]
pub struct FeeSplitterState {
    pub asset: Address,
    pub admin: Address,
    pub beneficiaries: Vec<Beneficiary>,
    pub primary_recipient: Address,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum FeeSplitterError {
    NotInitialized = 1,
    AlreadyInitialized = 2,
    InvalidBasisPoints = 3,
    NotAuthorized = 4,
    EmptyBeneficiaries = 5,
    InvalidAmount = 6,
}

#[contract]
pub struct FeeSplitter;

#[contractimpl]
impl FeeSplitter {
    pub fn initialize(
        env: Env,
        asset: Address,
        admin: Address,
        beneficiaries: Vec<Beneficiary>,
        primary_recipient: Address,
    ) -> Result<(), FeeSplitterError> {
        let storage = env.storage().persistent();

        if storage.has(&DataKey::Admin) {
            return Err(FeeSplitterError::AlreadyInitialized);
        }

        if beneficiaries.is_empty() {
            return Err(FeeSplitterError::EmptyBeneficiaries);
        }

        let mut total_bps: u32 = 0;
        for beneficiary in beneficiaries.iter() {
            total_bps = total_bps
                .checked_add(beneficiary.basis_points)
                .ok_or(FeeSplitterError::InvalidBasisPoints)?;
        }

        if total_bps != HUNDRED_PERCENT_BPS {
            return Err(FeeSplitterError::InvalidBasisPoints);
        }

        admin.require_auth();

        storage.set(&DataKey::Asset, &asset);
        storage.set(&DataKey::Admin, &admin);
        storage.set(&DataKey::Beneficiaries, &beneficiaries);
        storage.set(&DataKey::PrimaryRecipient, &primary_recipient);

        Ok(())
    }

    pub fn split_fee(env: Env, amount: i128) -> Result<(), FeeSplitterError> {
        let storage = env.storage().persistent();

        let asset: Address = storage
            .get(&DataKey::Asset)
            .ok_or(FeeSplitterError::NotInitialized)?;
        let beneficiaries: Vec<Beneficiary> = storage
            .get(&DataKey::Beneficiaries)
            .ok_or(FeeSplitterError::NotInitialized)?;
        let primary_recipient: Address = storage
            .get(&DataKey::PrimaryRecipient)
            .ok_or(FeeSplitterError::NotInitialized)?;

        if amount <= 0 {
            return Err(FeeSplitterError::InvalidAmount);
        }

        let token_client = token::Client::new(&env, &asset);
        let contract_address = Address::from_contract_id(&env, &env.current_contract_address());

        let mut remaining = amount;
        for (idx, beneficiary) in beneficiaries.iter().enumerate() {
            let share = (amount as u64)
                .checked_mul(beneficiary.basis_points as u64)
                .ok_or(FeeSplitterError::InvalidAmount)?
                / HUNDRED_PERCENT_BPS as u64;

            let share_i128 = share as i128;

            if share_i128 > 0 {
                token_client.transfer(&contract_address, &beneficiary.address, &share_i128);
            }

            remaining -= share_i128;

            if remaining <= 0 && idx < beneficiaries.len() - 1 {
                break;
            }
        }

        if remaining > 0 {
            token_client.transfer(&contract_address, &primary_recipient, &remaining);
        }

        env.events().publish(
            (symbol_short!("FeeSplitEx"),),
            (amount, beneficiaries.len()),
        );

        Ok(())
    }

    pub fn update_beneficiaries(
        env: Env,
        beneficiaries: Vec<Beneficiary>,
    ) -> Result<(), FeeSplitterError> {
        let storage = env.storage().persistent();

        let admin: Address = storage
            .get(&DataKey::Admin)
            .ok_or(FeeSplitterError::NotInitialized)?;

        admin.require_auth();

        if beneficiaries.is_empty() {
            return Err(FeeSplitterError::EmptyBeneficiaries);
        }

        let mut total_bps: u32 = 0;
        for beneficiary in beneficiaries.iter() {
            total_bps = total_bps
                .checked_add(beneficiary.basis_points)
                .ok_or(FeeSplitterError::InvalidBasisPoints)?;
        }

        if total_bps != HUNDRED_PERCENT_BPS {
            return Err(FeeSplitterError::InvalidBasisPoints);
        }

        storage.set(&DataKey::Beneficiaries, &beneficiaries);

        env.events().publish(
            (symbol_short!("BeneficiaryUpd"),),
            beneficiaries.len(),
        );

        Ok(())
    }

    pub fn get_state(env: Env) -> Result<FeeSplitterState, FeeSplitterError> {
        let storage = env.storage().persistent();

        let asset = storage
            .get(&DataKey::Asset)
            .ok_or(FeeSplitterError::NotInitialized)?;
        let admin = storage
            .get(&DataKey::Admin)
            .ok_or(FeeSplitterError::NotInitialized)?;
        let beneficiaries = storage
            .get(&DataKey::Beneficiaries)
            .ok_or(FeeSplitterError::NotInitialized)?;
        let primary_recipient = storage
            .get(&DataKey::PrimaryRecipient)
            .ok_or(FeeSplitterError::NotInitialized)?;

        Ok(FeeSplitterState {
            asset,
            admin,
            beneficiaries,
            primary_recipient,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use soroban_sdk::testutils::Address as _;

    #[test]
    fn test_initialize() {
        let env = Env::default();
        let contract_id = env.register_contract(None, FeeSplitter);

        let admin = Address::random(&env);
        let asset = Address::random(&env);
        let beneficiary1 = Address::random(&env);
        let beneficiary2 = Address::random(&env);
        let primary = Address::random(&env);

        let mut beneficiaries = Vec::new(&env);
        beneficiaries.push_back(Beneficiary {
            address: beneficiary1,
            basis_points: 6000,
        });
        beneficiaries.push_back(Beneficiary {
            address: beneficiary2,
            basis_points: 4000,
        });

        let client = soroban_sdk::contract::ContractClient::new(&env, &contract_id);
        let result = client.invoke(
            &symbol_short!("init"),
            &(&asset, &admin, &beneficiaries, &primary),
        );

        assert!(result.is_ok());
    }
}
