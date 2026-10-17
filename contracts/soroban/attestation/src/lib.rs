#![no_std]

use soroban_sdk::{
    contract, contracterror, contractimpl, contracttype, Address, Bytes, BytesN, Env, String,
    Symbol, Vec,
};

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod gas;
#[cfg(test)]
mod test;

const SECONDS_PER_DAY: u64 = 86_400;
const LEDGER_TTL_THRESHOLD: u32 = 518_400;
const LEDGER_TTL_EXTEND_TO: u32 = 3_110_400;

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Admin,
    Maintainer(Address),
    Attestation(u64),
}

/// A daily Merkle root of the reconciliation summaries and compliance logs.
/// `day` is the UTC day index (unix timestamp / 86400) the root covers.
#[derive(Clone, Debug, Eq, PartialEq)]
#[contracttype]
pub struct Attestation {
    pub merkle_root: BytesN<32>,
    pub published_at: u64,
    pub published_by: Address,
    pub metadata: String,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum AttestationError {
    NotInitialized = 1,
    AlreadyInitialized = 2,
    NotMaintainer = 3,
    AlreadyAttested = 4,
    NotFound = 5,
    FutureDay = 6,
}

#[contract]
pub struct AttestationContract;

#[contractimpl]
impl AttestationContract {
    pub fn initialize(env: Env, admin: Address) -> Result<(), AttestationError> {
        if env.storage().instance().has(&DataKey::Admin) {
            return Err(AttestationError::AlreadyInitialized);
        }

        admin.require_auth();
        env.storage().instance().set(&DataKey::Admin, &admin);
        bump_instance(&env);

        Ok(())
    }

    pub fn add_maintainer(env: Env, maintainer: Address) -> Result<(), AttestationError> {
        require_admin(&env)?;

        let key = DataKey::Maintainer(maintainer);
        env.storage().persistent().set(&key, &true);
        env.storage()
            .persistent()
            .extend_ttl(&key, LEDGER_TTL_THRESHOLD, LEDGER_TTL_EXTEND_TO);
        bump_instance(&env);

        Ok(())
    }

    pub fn remove_maintainer(env: Env, maintainer: Address) -> Result<(), AttestationError> {
        require_admin(&env)?;

        env.storage()
            .persistent()
            .remove(&DataKey::Maintainer(maintainer));
        bump_instance(&env);

        Ok(())
    }

    /// Commits the Merkle root for `day`. A day can be attested exactly once;
    /// the stored record can never be updated or overwritten.
    pub fn publish_attestation(
        env: Env,
        maintainer: Address,
        day: u64,
        merkle_root: BytesN<32>,
        metadata: String,
    ) -> Result<(), AttestationError> {
        if !env.storage().instance().has(&DataKey::Admin) {
            return Err(AttestationError::NotInitialized);
        }

        maintainer.require_auth();
        if !is_maintainer_key(&env, &maintainer) {
            return Err(AttestationError::NotMaintainer);
        }

        let published_at = env.ledger().timestamp();
        if day > published_at / SECONDS_PER_DAY {
            return Err(AttestationError::FutureDay);
        }

        let key = DataKey::Attestation(day);
        if env.storage().persistent().has(&key) {
            return Err(AttestationError::AlreadyAttested);
        }

        let record = Attestation {
            merkle_root: merkle_root.clone(),
            published_at,
            published_by: maintainer,
            metadata,
        };
        env.storage().persistent().set(&key, &record);
        env.storage()
            .persistent()
            .extend_ttl(&key, LEDGER_TTL_THRESHOLD, LEDGER_TTL_EXTEND_TO);
        bump_instance(&env);

        env.events().publish(
            (Symbol::new(&env, "AttestationPublished"), day),
            (merkle_root, published_at),
        );

        Ok(())
    }

    pub fn get_attestation(env: Env, day: u64) -> Result<Attestation, AttestationError> {
        env.storage()
            .persistent()
            .get(&DataKey::Attestation(day))
            .ok_or(AttestationError::NotFound)
    }

    /// Lets an external auditor check that `leaf` is part of the Merkle root
    /// committed for `day`. Sibling pairs are hashed in sorted order, so the
    /// proof is just the list of sibling hashes from leaf to root.
    pub fn verify_inclusion(
        env: Env,
        day: u64,
        leaf: BytesN<32>,
        proof: Vec<BytesN<32>>,
    ) -> Result<bool, AttestationError> {
        let record = Self::get_attestation(env.clone(), day)?;

        let mut computed = leaf;
        for sibling in proof.iter() {
            computed = hash_pair(&env, &computed, &sibling);
        }

        Ok(computed == record.merkle_root)
    }

    pub fn is_maintainer(env: Env, maintainer: Address) -> bool {
        is_maintainer_key(&env, &maintainer)
    }
}

fn bump_instance(env: &Env) {
    env.storage()
        .instance()
        .extend_ttl(LEDGER_TTL_THRESHOLD, LEDGER_TTL_EXTEND_TO);
}

fn require_admin(env: &Env) -> Result<(), AttestationError> {
    let admin: Address = env
        .storage()
        .instance()
        .get(&DataKey::Admin)
        .ok_or(AttestationError::NotInitialized)?;
    admin.require_auth();
    Ok(())
}

fn is_maintainer_key(env: &Env, maintainer: &Address) -> bool {
    let key = DataKey::Maintainer(maintainer.clone());
    if env.storage().persistent().has(&key) {
        env.storage()
            .persistent()
            .extend_ttl(&key, LEDGER_TTL_THRESHOLD, LEDGER_TTL_EXTEND_TO);
        true
    } else {
        false
    }
}

fn hash_pair(env: &Env, a: &BytesN<32>, b: &BytesN<32>) -> BytesN<32> {
    let (lo, hi) = if a.to_array() <= b.to_array() {
        (a, b)
    } else {
        (b, a)
    };

    let mut data = Bytes::from_array(env, &lo.to_array());
    data.append(&Bytes::from_array(env, &hi.to_array()));
    env.crypto().sha256(&data).to_bytes()
}
