#![no_std]

use soroban_sdk::{
    contract, contracterror, contractimpl, contracttype, symbol_short, token, Address, Env, Vec,
};

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Stream,
}

#[derive(Clone)]
#[contracttype]
pub struct StreamData {
    pub asset: Address,
    pub sender: Address,
    pub recipient: Address,
    pub total_amount: i128,
    pub start_ledger: u64,
    pub end_ledger: u64,
    pub claimed_amount: i128,
    pub cancelled: bool,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum StreamError {
    NotInitialized = 1,
    AlreadyInitialized = 2,
    InvalidDuration = 3,
    NotAuthorized = 4,
    AlreadyCancelled = 5,
    InvalidAmount = 6,
    ZeroClaim = 7,
}

#[contract]
pub struct PaymentStream;

#[contractimpl]
impl PaymentStream {
    pub fn initialize(
        env: Env,
        asset: Address,
        sender: Address,
        recipient: Address,
        total_amount: i128,
        start_ledger: u64,
        end_ledger: u64,
    ) -> Result<(), StreamError> {
        let storage = env.storage().persistent();

        if storage.has(&DataKey::Stream) {
            return Err(StreamError::AlreadyInitialized);
        }

        if end_ledger <= start_ledger || total_amount <= 0 {
            return Err(StreamError::InvalidDuration);
        }

        sender.require_auth();

        let stream = StreamData {
            asset: asset.clone(),
            sender: sender.clone(),
            recipient: recipient.clone(),
            total_amount,
            start_ledger,
            end_ledger,
            claimed_amount: 0,
            cancelled: false,
        };

        storage.set(&DataKey::Stream, &stream);

        let token_client = token::Client::new(&env, &asset);
        token_client.transfer(
            &sender,
            &env.current_contract_address(),
            &total_amount,
        );

        Ok(())
    }

    pub fn claimable_balance(env: Env) -> Result<i128, StreamError> {
        let storage = env.storage().persistent();
        let stream: StreamData = storage
            .get(&DataKey::Stream)
            .ok_or(StreamError::NotInitialized)?;

        if stream.cancelled {
            return Ok(0);
        }

        let current_ledger = u64::from(env.ledger().sequence());

        if current_ledger <= stream.start_ledger {
            return Ok(0);
        }

        if current_ledger >= stream.end_ledger {
            return Ok(stream.total_amount - stream.claimed_amount);
        }

        let elapsed = current_ledger - stream.start_ledger;
        let total_duration = stream.end_ledger - stream.start_ledger;

        let vested = (stream.total_amount as u64)
            .checked_mul(elapsed as u64)
            .ok_or(StreamError::InvalidAmount)?
            / total_duration as u64;

        let claimable = vested as i128 - stream.claimed_amount;

        Ok(if claimable < 0 { 0 } else { claimable })
    }

    pub fn withdraw(env: Env, amount: i128) -> Result<(), StreamError> {
        let storage = env.storage().persistent();

        let mut stream: StreamData = storage
            .get(&DataKey::Stream)
            .ok_or(StreamError::NotInitialized)?;

        if stream.cancelled {
            return Err(StreamError::AlreadyCancelled);
        }

        if amount <= 0 {
            return Err(StreamError::ZeroClaim);
        }

        stream.recipient.require_auth();

        let claimable = Self::claimable_balance(env.clone())?;

        if amount > claimable {
            return Err(StreamError::InvalidAmount);
        }

        stream.claimed_amount += amount;

        let token_client = token::Client::new(&env, &stream.asset);
        token_client.transfer(
            &env.current_contract_address(),
            &stream.recipient,
            &amount,
        );

        storage.set(&DataKey::Stream, &stream);

        Ok(())
    }

    pub fn cancel(env: Env) -> Result<(), StreamError> {
        let storage = env.storage().persistent();

        let mut stream: StreamData = storage
            .get(&DataKey::Stream)
            .ok_or(StreamError::NotInitialized)?;

        if stream.cancelled {
            return Err(StreamError::AlreadyCancelled);
        }

        stream.sender.require_auth();

        let claimable = Self::claimable_balance(env.clone())?;
        let remaining = stream.total_amount - stream.claimed_amount;
        let unvested = remaining - claimable;

        stream.cancelled = true;

        let token_client = token::Client::new(&env, &stream.asset);
        let contract_address = env.current_contract_address();

        if claimable > 0 {
            token_client.transfer(&contract_address, &stream.recipient, &claimable);
        }

        if unvested > 0 {
            token_client.transfer(&contract_address, &stream.sender, &unvested);
        }

        storage.set(&DataKey::Stream, &stream);

        Ok(())
    }

    pub fn get_stream(env: Env) -> Result<StreamData, StreamError> {
        let storage = env.storage().persistent();
        storage
            .get(&DataKey::Stream)
            .ok_or(StreamError::NotInitialized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use soroban_sdk::testutils::Address as _;

    #[test]
    fn test_initialize() {
        let env = Env::default();
        let contract_id = env.register_contract(None, PaymentStream);

        let sender = Address::random(&env);
        let recipient = Address::random(&env);
        let asset = Address::random(&env);

        let client = soroban_sdk::contract::ContractClient::new(&env, &contract_id);
        let result = client.invoke(
            &symbol_short!("init"),
            &(&asset, &sender, &recipient, &1000i128, &10u64, &100u64),
        );

        assert!(result.is_ok());
    }
}
