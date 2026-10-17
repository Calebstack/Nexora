#![no_std]

use soroban_sdk::{
    contract, contracterror, contractimpl, contracttype, token, xdr::ToXdr, Address, Bytes, BytesN,
    Env, Symbol, Vec,
};

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod gas;
#[cfg(test)]
mod test;

const LEDGER_TTL_THRESHOLD: u32 = 518_400;
const LEDGER_TTL_EXTEND_TO: u32 = 3_110_400;

const RELEASE_DOMAIN: &[u8] = b"NEXORA_BRIDGE_RELEASE";
const ROTATE_DOMAIN: &[u8] = b"NEXORA_BRIDGE_ROTATE";

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Admin,
    Token,
    MintMode,
    Validators,
    Threshold,
    Paused,
    UsedNonce(u64),
}

/// An Ed25519 signature from one member of the validator set over the
/// payload built by `release_payload` or `rotation_payload`.
#[derive(Clone, Debug, Eq, PartialEq)]
#[contracttype]
pub struct ValidatorSignature {
    pub validator: BytesN<32>,
    pub signature: BytesN<64>,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum BridgeError {
    NotInitialized = 1,
    AlreadyInitialized = 2,
    BridgePaused = 3,
    InvalidAmount = 4,
    NonceAlreadyUsed = 5,
    InvalidValidatorSet = 6,
    InvalidThreshold = 7,
    UnknownValidator = 8,
    DuplicateSigner = 9,
    InsufficientSignatures = 10,
}

#[contract]
pub struct BridgeAdapter;

#[contractimpl]
impl BridgeAdapter {
    /// `mint_mode` selects how wrapped assets move: when true the adapter must
    /// be the admin of the Stellar Asset Contract, deposits burn and releases
    /// mint; when false the adapter holds a reserve, deposits lock funds in
    /// it and releases pay out of it.
    pub fn initialize(
        env: Env,
        admin: Address,
        token: Address,
        mint_mode: bool,
        validators: Vec<BytesN<32>>,
        threshold: u32,
    ) -> Result<(), BridgeError> {
        if env.storage().instance().has(&DataKey::Admin) {
            return Err(BridgeError::AlreadyInitialized);
        }

        validate_validator_set(&validators, threshold)?;
        admin.require_auth();

        let storage = env.storage().instance();
        storage.set(&DataKey::Admin, &admin);
        storage.set(&DataKey::Token, &token);
        storage.set(&DataKey::MintMode, &mint_mode);
        storage.set(&DataKey::Validators, &validators);
        storage.set(&DataKey::Threshold, &threshold);
        storage.set(&DataKey::Paused, &false);
        bump_instance(&env);

        Ok(())
    }

    /// Moves `amount` from `from` into the bridge and records the destination
    /// on the settlement network for the validators to pick up.
    pub fn deposit(
        env: Env,
        from: Address,
        amount: i128,
        destination: Bytes,
    ) -> Result<(), BridgeError> {
        require_initialized(&env)?;
        require_not_paused(&env)?;
        if amount <= 0 {
            return Err(BridgeError::InvalidAmount);
        }

        from.require_auth();

        let token_client = token::Client::new(&env, &token_address(&env)?);
        if is_mint_mode(&env) {
            token_client.burn(&from, &amount);
        } else {
            token_client.transfer(&from, &env.current_contract_address(), &amount);
        }
        bump_instance(&env);

        env.events()
            .publish((Symbol::new(&env, "Deposit"), from), (amount, destination));

        Ok(())
    }

    /// Mints or releases `amount` to `recipient` once a quorum of validators
    /// has signed the transfer. Each `nonce` can be used exactly once.
    pub fn release(
        env: Env,
        recipient: Address,
        amount: i128,
        nonce: u64,
        signatures: Vec<ValidatorSignature>,
    ) -> Result<(), BridgeError> {
        require_initialized(&env)?;
        require_not_paused(&env)?;
        if amount <= 0 {
            return Err(BridgeError::InvalidAmount);
        }
        require_unused_nonce(&env, nonce)?;

        let payload = release_payload(&env, &recipient, amount, nonce);
        verify_quorum(&env, &payload, &signatures)?;
        consume_nonce(&env, nonce);

        let token_address = token_address(&env)?;
        if is_mint_mode(&env) {
            token::StellarAssetClient::new(&env, &token_address).mint(&recipient, &amount);
        } else {
            token::Client::new(&env, &token_address).transfer(
                &env.current_contract_address(),
                &recipient,
                &amount,
            );
        }
        bump_instance(&env);

        env.events()
            .publish((Symbol::new(&env, "Release"), recipient), (amount, nonce));

        Ok(())
    }

    /// Replaces the validator set and threshold. The change has to be signed
    /// by a quorum of the current set, so a rotation is bound by the same
    /// trust model as a release. It stays available while the bridge is paused
    /// so a compromised set can be replaced during an incident.
    pub fn rotate_validators(
        env: Env,
        new_validators: Vec<BytesN<32>>,
        new_threshold: u32,
        nonce: u64,
        signatures: Vec<ValidatorSignature>,
    ) -> Result<(), BridgeError> {
        require_initialized(&env)?;
        validate_validator_set(&new_validators, new_threshold)?;
        require_unused_nonce(&env, nonce)?;

        let payload = rotation_payload(&env, &new_validators, new_threshold, nonce);
        verify_quorum(&env, &payload, &signatures)?;
        consume_nonce(&env, nonce);

        let storage = env.storage().instance();
        storage.set(&DataKey::Validators, &new_validators);
        storage.set(&DataKey::Threshold, &new_threshold);
        bump_instance(&env);

        env.events().publish(
            (Symbol::new(&env, "ValidatorsRotated"),),
            (new_validators, new_threshold),
        );

        Ok(())
    }

    /// Emergency shutdown toggle: pauses or resumes deposits and withdrawals.
    pub fn set_paused(env: Env, paused: bool) -> Result<(), BridgeError> {
        let admin: Address = env
            .storage()
            .instance()
            .get(&DataKey::Admin)
            .ok_or(BridgeError::NotInitialized)?;
        admin.require_auth();

        env.storage().instance().set(&DataKey::Paused, &paused);
        bump_instance(&env);

        env.events()
            .publish((Symbol::new(&env, "PausedChanged"),), paused);

        Ok(())
    }

    pub fn is_paused(env: Env) -> bool {
        env.storage()
            .instance()
            .get(&DataKey::Paused)
            .unwrap_or(false)
    }

    pub fn get_validators(env: Env) -> Result<Vec<BytesN<32>>, BridgeError> {
        env.storage()
            .instance()
            .get(&DataKey::Validators)
            .ok_or(BridgeError::NotInitialized)
    }

    pub fn get_threshold(env: Env) -> Result<u32, BridgeError> {
        env.storage()
            .instance()
            .get(&DataKey::Threshold)
            .ok_or(BridgeError::NotInitialized)
    }

    pub fn is_nonce_used(env: Env, nonce: u64) -> bool {
        env.storage().persistent().has(&DataKey::UsedNonce(nonce))
    }
}

/// The exact bytes validators sign to approve a release. It binds the network,
/// this contract, the recipient, the amount and the nonce, so a signature
/// cannot be replayed on another network, another deployment or another
/// transfer.
pub fn release_payload(env: &Env, recipient: &Address, amount: i128, nonce: u64) -> Bytes {
    let mut payload = domain_prefix(env, RELEASE_DOMAIN);
    payload.append(&recipient.to_xdr(env));
    payload.append(&Bytes::from_array(env, &amount.to_be_bytes()));
    payload.append(&Bytes::from_array(env, &nonce.to_be_bytes()));
    payload
}

/// The exact bytes validators sign to approve a validator set rotation.
pub fn rotation_payload(
    env: &Env,
    new_validators: &Vec<BytesN<32>>,
    new_threshold: u32,
    nonce: u64,
) -> Bytes {
    let mut payload = domain_prefix(env, ROTATE_DOMAIN);
    payload.append(&Bytes::from_array(env, &new_threshold.to_be_bytes()));
    payload.append(&Bytes::from_array(env, &nonce.to_be_bytes()));
    for validator in new_validators.iter() {
        payload.append(&Bytes::from_array(env, &validator.to_array()));
    }
    payload
}

fn domain_prefix(env: &Env, domain: &[u8]) -> Bytes {
    let mut prefix = Bytes::from_slice(env, domain);
    prefix.append(&Bytes::from_array(env, &env.ledger().network_id().to_array()));
    prefix.append(&env.current_contract_address().to_xdr(env));
    prefix
}

fn validate_validator_set(validators: &Vec<BytesN<32>>, threshold: u32) -> Result<(), BridgeError> {
    let count = validators.len();
    if count == 0 {
        return Err(BridgeError::InvalidValidatorSet);
    }
    // The threshold has to be a strict majority of the set, otherwise two
    // disjoint groups of validators could each approve conflicting transfers.
    if threshold == 0 || threshold > count || (threshold as u64) * 2 <= count as u64 {
        return Err(BridgeError::InvalidThreshold);
    }

    for i in 0..count {
        for j in (i + 1)..count {
            if validators.get_unchecked(i) == validators.get_unchecked(j) {
                return Err(BridgeError::InvalidValidatorSet);
            }
        }
    }

    Ok(())
}

fn verify_quorum(
    env: &Env,
    payload: &Bytes,
    signatures: &Vec<ValidatorSignature>,
) -> Result<(), BridgeError> {
    let validators: Vec<BytesN<32>> = env
        .storage()
        .instance()
        .get(&DataKey::Validators)
        .ok_or(BridgeError::NotInitialized)?;
    let threshold: u32 = env
        .storage()
        .instance()
        .get(&DataKey::Threshold)
        .ok_or(BridgeError::NotInitialized)?;

    let mut seen: Vec<BytesN<32>> = Vec::new(env);
    for entry in signatures.iter() {
        if !validators.contains(&entry.validator) {
            return Err(BridgeError::UnknownValidator);
        }
        if seen.contains(&entry.validator) {
            return Err(BridgeError::DuplicateSigner);
        }
        // Traps the invocation if the signature does not match.
        env.crypto()
            .ed25519_verify(&entry.validator, payload, &entry.signature);
        seen.push_back(entry.validator);
    }

    if seen.len() < threshold {
        return Err(BridgeError::InsufficientSignatures);
    }

    Ok(())
}

fn require_unused_nonce(env: &Env, nonce: u64) -> Result<(), BridgeError> {
    if env.storage().persistent().has(&DataKey::UsedNonce(nonce)) {
        return Err(BridgeError::NonceAlreadyUsed);
    }
    Ok(())
}

fn consume_nonce(env: &Env, nonce: u64) {
    let key = DataKey::UsedNonce(nonce);
    env.storage().persistent().set(&key, &true);
    env.storage()
        .persistent()
        .extend_ttl(&key, LEDGER_TTL_THRESHOLD, LEDGER_TTL_EXTEND_TO);
}

fn require_initialized(env: &Env) -> Result<(), BridgeError> {
    if env.storage().instance().has(&DataKey::Admin) {
        Ok(())
    } else {
        Err(BridgeError::NotInitialized)
    }
}

fn require_not_paused(env: &Env) -> Result<(), BridgeError> {
    if BridgeAdapter::is_paused(env.clone()) {
        Err(BridgeError::BridgePaused)
    } else {
        Ok(())
    }
}

fn token_address(env: &Env) -> Result<Address, BridgeError> {
    env.storage()
        .instance()
        .get(&DataKey::Token)
        .ok_or(BridgeError::NotInitialized)
}

fn is_mint_mode(env: &Env) -> bool {
    env.storage()
        .instance()
        .get(&DataKey::MintMode)
        .unwrap_or(false)
}

fn bump_instance(env: &Env) {
    env.storage()
        .instance()
        .extend_ttl(LEDGER_TTL_THRESHOLD, LEDGER_TTL_EXTEND_TO);
}
