#![no_std]

use soroban_sdk::{
    contract, contracterror, contractimpl, contracttype, symbol_short, token, Address, Env,
    Vec, Map,
};

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Escrow,
}

#[derive(Clone)]
#[contracttype]
pub struct EscrowData {
    pub asset: Address,
    pub sender: Address,
    pub recipient: Address,
    pub amount: i128,
    pub threshold: u32,
    pub signers_count: u32,
    pub signatures: Vec<Address>,
    pub deadline: u64,
    pub released: bool,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum EscrowError {
    NotInitialized = 1,
    AlreadyInitialized = 2,
    InvalidThreshold = 3,
    NotAuthorized = 4,
    DeadlineExceeded = 5,
    AlreadyReleased = 6,
    InsufficientSignatures = 7,
    AlreadySigned = 8,
}

#[contract]
pub struct Escrow;

#[contractimpl]
impl Escrow {
    pub fn initialize(
        env: Env,
        asset: Address,
        sender: Address,
        recipient: Address,
        amount: i128,
        threshold: u32,
        signers_count: u32,
        deadline: u64,
    ) -> Result<(), EscrowError> {
        let storage = env.storage().persistent();

        if storage.has(&DataKey::Escrow) {
            return Err(EscrowError::AlreadyInitialized);
        }

        if threshold == 0 || threshold > signers_count {
            return Err(EscrowError::InvalidThreshold);
        }

        sender.require_auth();

        let escrow = EscrowData {
            asset: asset.clone(),
            sender: sender.clone(),
            recipient: recipient.clone(),
            amount,
            threshold,
            signers_count,
            signatures: Vec::new(&env),
            deadline,
            released: false,
        };

        storage.set(&DataKey::Escrow, &escrow);

        let token_client = token::Client::new(&env, &asset);
        token_client.transfer(&sender, &Address::from_contract_id(&env, &env.current_contract_address()), &amount);

        Ok(())
    }

    pub fn sign(env: Env, signer: Address) -> Result<(), EscrowError> {
        let storage = env.storage().persistent();

        let mut escrow: EscrowData = storage
            .get(&DataKey::Escrow)
            .ok_or(EscrowError::NotInitialized)?;

        if escrow.released {
            return Err(EscrowError::AlreadyReleased);
        }

        let current_ledger = env.ledger().sequence();
        if current_ledger > escrow.deadline {
            return Err(EscrowError::DeadlineExceeded);
        }

        signer.require_auth();

        for sig in escrow.signatures.iter() {
            if sig == signer {
                return Err(EscrowError::AlreadySigned);
            }
        }

        escrow.signatures.push_back(signer);

        if escrow.signatures.len() as u32 >= escrow.threshold {
            escrow.released = true;
            let token_client = token::Client::new(&env, &escrow.asset);
            token_client.transfer(
                &Address::from_contract_id(&env, &env.current_contract_address()),
                &escrow.recipient,
                &escrow.amount,
            );
        }

        storage.set(&DataKey::Escrow, &escrow);
        Ok(())
    }

    pub fn refund(env: Env) -> Result<(), EscrowError> {
        let storage = env.storage().persistent();

        let mut escrow: EscrowData = storage
            .get(&DataKey::Escrow)
            .ok_or(EscrowError::NotInitialized)?;

        if escrow.released {
            return Err(EscrowError::AlreadyReleased);
        }

        let current_ledger = env.ledger().sequence();
        if current_ledger <= escrow.deadline {
            return Err(EscrowError::DeadlineExceeded);
        }

        escrow.sender.require_auth();

        escrow.released = true;
        let token_client = token::Client::new(&env, &escrow.asset);
        token_client.transfer(
            &Address::from_contract_id(&env, &env.current_contract_address()),
            &escrow.sender,
            &escrow.amount,
        );

        storage.set(&DataKey::Escrow, &escrow);
        Ok(())
    }

    pub fn get_escrow(env: Env) -> Result<EscrowData, EscrowError> {
        let storage = env.storage().persistent();
        storage
            .get(&DataKey::Escrow)
            .ok_or(EscrowError::NotInitialized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use soroban_sdk::testutils::{Address as _, Ledger};

    #[test]
    fn test_initialize() {
        let env = Env::default();
        let contract_id = env.register_contract(None, Escrow);
        let client = soroban_sdk::contract::ContractClient::new(&env, &contract_id);

        let sender = Address::random(&env);
        let recipient = Address::random(&env);
        let asset = Address::random(&env);

        let result = client.invoke(
            &symbol_short!("init"),
            &(&asset, &sender, &recipient, &1000i128, &2u32, &3u32, &100u64),
        );

        assert!(result.is_ok());
    }
}
