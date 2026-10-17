#![no_std]

use soroban_sdk::{
    contract, contracterror, contractimpl, contracttype, symbol_short, token, xdr::ToXdr, Address,
    Bytes, BytesN, Env, Vec,
};

/// Claimed flags are packed 128 to a storage entry so a large airdrop only
/// pays rent for one entry per 128 recipients.
const BITS_PER_WORD: u32 = 128;

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Config,
    ClaimedWord(u32),
}

#[derive(Clone)]
#[contracttype]
pub struct DistributorConfig {
    pub root: BytesN<32>,
    pub token: Address,
    pub sender: Address,
    pub expiration_ledger: u32,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum DistributorError {
    NotInitialized = 1,
    InvalidExpiration = 2,
    InvalidAmount = 3,
    AlreadyClaimed = 4,
    InvalidProof = 5,
    ClaimPeriodEnded = 6,
    ClaimPeriodActive = 7,
}

#[contract]
pub struct MerkleDistributor;

#[contractimpl]
impl MerkleDistributor {
    /// Runs at deployment so the root can never be set by a later caller.
    /// Claims are accepted through `expiration_ledger`; after it the `sender`
    /// may claw back whatever is left.
    pub fn __constructor(
        env: Env,
        root: BytesN<32>,
        token: Address,
        sender: Address,
        expiration_ledger: u32,
    ) -> Result<(), DistributorError> {
        if expiration_ledger <= env.ledger().sequence() {
            return Err(DistributorError::InvalidExpiration);
        }
        env.storage().instance().set(
            &DataKey::Config,
            &DistributorConfig {
                root,
                token,
                sender,
                expiration_ledger,
            },
        );
        Ok(())
    }

    pub fn get_config(env: Env) -> Result<DistributorConfig, DistributorError> {
        Self::load(&env)
    }

    pub fn is_claimed(env: Env, index: u32) -> bool {
        Self::claimed(&env, index)
    }

    /// Pays `amount` to `recipient` if `proof` ties (index, recipient, amount)
    /// to the committed root and the index has not been claimed before.
    pub fn claim(
        env: Env,
        index: u32,
        recipient: Address,
        amount: i128,
        proof: Vec<BytesN<32>>,
    ) -> Result<(), DistributorError> {
        recipient.require_auth();

        let config = Self::load(&env)?;
        if env.ledger().sequence() > config.expiration_ledger {
            return Err(DistributorError::ClaimPeriodEnded);
        }
        if amount <= 0 {
            return Err(DistributorError::InvalidAmount);
        }
        if Self::claimed(&env, index) {
            return Err(DistributorError::AlreadyClaimed);
        }

        let leaf = leaf_hash(&env, index, &recipient, amount);
        if !verify_proof(&env, &config.root, leaf, &proof) {
            return Err(DistributorError::InvalidProof);
        }

        // Mark the index before moving funds so the transfer cannot re-enter
        // an unclaimed slot.
        Self::set_claimed(&env, index);

        token::Client::new(&env, &config.token).transfer(
            &env.current_contract_address(),
            &recipient,
            &amount,
        );

        env.events()
            .publish((symbol_short!("claimed"), recipient), (index, amount));
        Ok(())
    }

    /// Returns the unclaimed balance to the sender once the claim period is
    /// over. Returns the amount recovered.
    pub fn clawback(env: Env) -> Result<i128, DistributorError> {
        let config = Self::load(&env)?;
        config.sender.require_auth();

        if env.ledger().sequence() <= config.expiration_ledger {
            return Err(DistributorError::ClaimPeriodActive);
        }

        let token_client = token::Client::new(&env, &config.token);
        let remaining = token_client.balance(&env.current_contract_address());
        if remaining > 0 {
            token_client.transfer(&env.current_contract_address(), &config.sender, &remaining);
        }
        Ok(remaining)
    }

    fn load(env: &Env) -> Result<DistributorConfig, DistributorError> {
        env.storage()
            .instance()
            .get(&DataKey::Config)
            .ok_or(DistributorError::NotInitialized)
    }

    fn claimed(env: &Env, index: u32) -> bool {
        let word: u128 = env
            .storage()
            .persistent()
            .get(&DataKey::ClaimedWord(index / BITS_PER_WORD))
            .unwrap_or(0);
        word & (1u128 << (index % BITS_PER_WORD)) != 0
    }

    fn set_claimed(env: &Env, index: u32) {
        let key = DataKey::ClaimedWord(index / BITS_PER_WORD);
        let word: u128 = env.storage().persistent().get(&key).unwrap_or(0);
        env.storage()
            .persistent()
            .set(&key, &(word | (1u128 << (index % BITS_PER_WORD))));
    }
}

/// Leaf = sha256(index_be32 || xdr(recipient) || amount_be128). The Go tree
/// builder in internal/claimable/merkle.go encodes it identically.
pub fn leaf_hash(env: &Env, index: u32, recipient: &Address, amount: i128) -> BytesN<32> {
    let mut data = Bytes::new(env);
    data.extend_from_array(&index.to_be_bytes());
    data.append(&recipient.clone().to_xdr(env));
    data.extend_from_array(&amount.to_be_bytes());
    env.crypto().sha256(&data).to_bytes()
}

/// Parents hash their children in sorted order, so a proof is just the list of
/// sibling hashes with no left/right flags.
pub fn hash_pair(env: &Env, a: &BytesN<32>, b: &BytesN<32>) -> BytesN<32> {
    let (low, high) = if a <= b { (a, b) } else { (b, a) };
    let mut data = Bytes::new(env);
    data.extend_from_array(&low.to_array());
    data.extend_from_array(&high.to_array());
    env.crypto().sha256(&data).to_bytes()
}

fn verify_proof(env: &Env, root: &BytesN<32>, leaf: BytesN<32>, proof: &Vec<BytesN<32>>) -> bool {
    let mut node = leaf;
    for sibling in proof.iter() {
        node = hash_pair(env, &node, &sibling);
    }
    &node == root
}

#[cfg(test)]
mod test;
