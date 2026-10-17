use super::*;
use ed25519_dalek::{Signer, SigningKey};
use soroban_sdk::{
    testutils::Address as _,
    token::{Client as TokenClient, StellarAssetClient},
    Address, Bytes, BytesN, Env, Vec,
};

pub(crate) const RESERVE: i128 = 1_000_000;

pub(crate) struct Bridge<'a> {
    pub env: &'a Env,
    pub client: BridgeAdapterClient<'a>,
    pub id: Address,
    pub token: Address,
    pub keys: std::vec::Vec<SigningKey>,
}

pub(crate) fn signing_keys(count: u8) -> std::vec::Vec<SigningKey> {
    (1..=count).map(|n| SigningKey::from_bytes(&[n; 32])).collect()
}

pub(crate) fn public_keys(env: &Env, keys: &[SigningKey]) -> Vec<BytesN<32>> {
    let mut out = Vec::new(env);
    for key in keys {
        out.push_back(BytesN::from_array(env, &key.verifying_key().to_bytes()));
    }
    out
}

/// Deploys the adapter with `validators` validators and the given threshold.
/// In lock mode the adapter is funded with `RESERVE`; in mint mode it becomes
/// the admin of the asset instead.
pub(crate) fn setup<'a>(
    env: &'a Env,
    mint_mode: bool,
    validators: u8,
    threshold: u32,
) -> Bridge<'a> {
    env.mock_all_auths();

    let admin = Address::generate(env);
    let token = env.register_stellar_asset_contract_v2(admin.clone()).address();
    let id = env.register(BridgeAdapter, ());
    let client = BridgeAdapterClient::new(env, &id);
    let keys = signing_keys(validators);

    let asset = StellarAssetClient::new(env, &token);
    if mint_mode {
        asset.set_admin(&id);
    } else {
        asset.mint(&id, &RESERVE);
    }

    client.initialize(&admin, &token, &mint_mode, &public_keys(env, &keys), &threshold);

    Bridge {
        env,
        client,
        id,
        token,
        keys,
    }
}

impl<'a> Bridge<'a> {
    pub fn release_signatures(
        &self,
        signers: &[usize],
        recipient: &Address,
        amount: i128,
        nonce: u64,
    ) -> Vec<ValidatorSignature> {
        let payload = self
            .env
            .as_contract(&self.id, || release_payload(self.env, recipient, amount, nonce));
        self.sign_with(&self.keys, signers, &payload)
    }

    pub fn rotation_signatures(
        &self,
        signers: &[usize],
        new_validators: &Vec<BytesN<32>>,
        new_threshold: u32,
        nonce: u64,
    ) -> Vec<ValidatorSignature> {
        let payload = self.env.as_contract(&self.id, || {
            rotation_payload(self.env, new_validators, new_threshold, nonce)
        });
        self.sign_with(&self.keys, signers, &payload)
    }

    /// Signs `payload` with the keys at `signers` in `keys`, which lets tests
    /// sign as validators that are not (or no longer) in the adapter's set.
    pub fn sign_with(
        &self,
        keys: &[SigningKey],
        signers: &[usize],
        payload: &Bytes,
    ) -> Vec<ValidatorSignature> {
        let message = payload.to_alloc_vec();
        let mut out = Vec::new(self.env);
        for &index in signers {
            let key = &keys[index];
            out.push_back(ValidatorSignature {
                validator: BytesN::from_array(self.env, &key.verifying_key().to_bytes()),
                signature: BytesN::from_array(self.env, &key.sign(&message).to_bytes()),
            });
        }
        out
    }

    pub fn balance(&self, who: &Address) -> i128 {
        TokenClient::new(self.env, &self.token).balance(who)
    }
}

#[test]
fn release_pays_out_of_reserve_with_quorum() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let recipient = Address::generate(&env);

    let sigs = bridge.release_signatures(&[0, 2], &recipient, 500, 1);
    bridge.client.release(&recipient, &500, &1, &sigs);

    assert_eq!(bridge.balance(&recipient), 500);
    assert_eq!(bridge.balance(&bridge.id), RESERVE - 500);
    assert!(bridge.client.is_nonce_used(&1));
}

#[test]
fn release_mints_in_mint_mode() {
    let env = Env::default();
    let bridge = setup(&env, true, 3, 2);
    let recipient = Address::generate(&env);

    let sigs = bridge.release_signatures(&[1, 2], &recipient, 750, 9);
    bridge.client.release(&recipient, &750, &9, &sigs);

    assert_eq!(bridge.balance(&recipient), 750);
}

#[test]
fn nonce_cannot_be_replayed() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let recipient = Address::generate(&env);

    let sigs = bridge.release_signatures(&[0, 1], &recipient, 100, 5);
    bridge.client.release(&recipient, &100, &5, &sigs);
    let replay = bridge.client.try_release(&recipient, &100, &5, &sigs);

    assert_eq!(replay, Err(Ok(BridgeError::NonceAlreadyUsed)));
    assert_eq!(bridge.balance(&recipient), 100);
}

#[test]
fn signatures_are_bound_to_amount_recipient_and_nonce() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let recipient = Address::generate(&env);
    let attacker = Address::generate(&env);

    let sigs = bridge.release_signatures(&[0, 1], &recipient, 100, 5);

    assert!(bridge.client.try_release(&recipient, &101, &5, &sigs).is_err());
    assert!(bridge.client.try_release(&attacker, &100, &5, &sigs).is_err());
    assert!(bridge.client.try_release(&recipient, &100, &6, &sigs).is_err());
    assert!(!bridge.client.is_nonce_used(&5));
}

#[test]
fn corrupted_signature_is_rejected() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let recipient = Address::generate(&env);

    let good = bridge.release_signatures(&[0, 1], &recipient, 100, 1);
    let mut corrupted = Vec::new(&env);
    for (i, entry) in good.iter().enumerate() {
        if i == 1 {
            let mut raw = entry.signature.to_array();
            raw[0] ^= 0xff;
            corrupted.push_back(ValidatorSignature {
                validator: entry.validator,
                signature: BytesN::from_array(&env, &raw),
            });
        } else {
            corrupted.push_back(entry);
        }
    }

    assert!(bridge.client.try_release(&recipient, &100, &1, &corrupted).is_err());
    assert!(!bridge.client.is_nonce_used(&1));
    assert_eq!(bridge.balance(&recipient), 0);
}

#[test]
fn duplicate_signatures_do_not_count_twice() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let recipient = Address::generate(&env);

    let sigs = bridge.release_signatures(&[0, 0], &recipient, 100, 1);
    let result = bridge.client.try_release(&recipient, &100, &1, &sigs);

    assert_eq!(result, Err(Ok(BridgeError::DuplicateSigner)));
}

#[test]
fn under_threshold_signature_set_is_rejected() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let recipient = Address::generate(&env);

    let sigs = bridge.release_signatures(&[1], &recipient, 100, 1);
    let result = bridge.client.try_release(&recipient, &100, &1, &sigs);

    assert_eq!(result, Err(Ok(BridgeError::InsufficientSignatures)));
    assert!(!bridge.client.is_nonce_used(&1));
}

#[test]
fn signatures_from_outside_the_validator_set_are_rejected() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let recipient = Address::generate(&env);

    // A key that was never registered signs the transfer.
    let outsider = [SigningKey::from_bytes(&[99; 32])];
    let payload = env.as_contract(&bridge.id, || release_payload(&env, &recipient, 100, 1));
    let sigs = bridge.sign_with(&outsider, &[0], &payload);
    let result = bridge.client.try_release(&recipient, &100, &1, &sigs);

    assert_eq!(result, Err(Ok(BridgeError::UnknownValidator)));
}

#[test]
fn rotation_replaces_the_validator_set() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let recipient = Address::generate(&env);

    let new_keys: std::vec::Vec<SigningKey> =
        (10..=14u8).map(|n| SigningKey::from_bytes(&[n; 32])).collect();
    let new_set = public_keys(&env, &new_keys);
    let rotation = bridge.rotation_signatures(&[0, 1], &new_set, 3, 1);
    bridge.client.rotate_validators(&new_set, &3, &1, &rotation);

    assert_eq!(bridge.client.get_validators(), new_set);
    assert_eq!(bridge.client.get_threshold(), 3);

    // The old validators can no longer approve releases.
    let old = bridge.release_signatures(&[0, 1], &recipient, 100, 2);
    assert!(bridge.client.try_release(&recipient, &100, &2, &old).is_err());

    // The new set can, once it reaches the new threshold.
    let payload = env.as_contract(&bridge.id, || release_payload(&env, &recipient, 100, 2));
    let two = bridge.sign_with(&new_keys, &[0, 1], &payload);
    assert_eq!(
        bridge.client.try_release(&recipient, &100, &2, &two),
        Err(Ok(BridgeError::InsufficientSignatures))
    );
    let three = bridge.sign_with(&new_keys, &[0, 1, 4], &payload);
    bridge.client.release(&recipient, &100, &2, &three);
    assert_eq!(bridge.balance(&recipient), 100);
}

#[test]
fn rotation_needs_a_quorum_of_the_current_set() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);

    let new_keys = signing_keys(5);
    let new_set = public_keys(&env, &new_keys);
    let rotation = bridge.rotation_signatures(&[0], &new_set, 3, 1);
    let result = bridge.client.try_rotate_validators(&new_set, &3, &1, &rotation);

    assert_eq!(result, Err(Ok(BridgeError::InsufficientSignatures)));
    assert_eq!(bridge.client.get_threshold(), 2);
}

#[test]
fn rotation_enforces_a_majority_threshold() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);

    let new_set = public_keys(&env, &signing_keys(4));
    let rotation = bridge.rotation_signatures(&[0, 1], &new_set, 2, 1);
    let result = bridge.client.try_rotate_validators(&new_set, &2, &1, &rotation);

    assert_eq!(result, Err(Ok(BridgeError::InvalidThreshold)));
}

#[test]
fn rotation_consumes_the_nonce() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let recipient = Address::generate(&env);

    let same_set = public_keys(&env, &bridge.keys);
    let rotation = bridge.rotation_signatures(&[0, 1], &same_set, 2, 7);
    bridge.client.rotate_validators(&same_set, &2, &7, &rotation);

    let sigs = bridge.release_signatures(&[0, 1], &recipient, 100, 7);
    let result = bridge.client.try_release(&recipient, &100, &7, &sigs);

    assert_eq!(result, Err(Ok(BridgeError::NonceAlreadyUsed)));
}

#[test]
fn initialize_validates_the_validator_set() {
    let env = Env::default();
    env.mock_all_auths();
    let admin = Address::generate(&env);
    let token = env.register_stellar_asset_contract_v2(admin.clone()).address();
    let id = env.register(BridgeAdapter, ());
    let client = BridgeAdapterClient::new(&env, &id);
    let set = public_keys(&env, &signing_keys(3));

    assert_eq!(
        client.try_initialize(&admin, &token, &false, &set, &1),
        Err(Ok(BridgeError::InvalidThreshold))
    );
    assert_eq!(
        client.try_initialize(&admin, &token, &false, &set, &4),
        Err(Ok(BridgeError::InvalidThreshold))
    );
    assert_eq!(
        client.try_initialize(&admin, &token, &false, &Vec::new(&env), &1),
        Err(Ok(BridgeError::InvalidValidatorSet))
    );

    let mut duplicated = Vec::new(&env);
    duplicated.push_back(set.get_unchecked(0));
    duplicated.push_back(set.get_unchecked(0));
    duplicated.push_back(set.get_unchecked(1));
    assert_eq!(
        client.try_initialize(&admin, &token, &false, &duplicated, &2),
        Err(Ok(BridgeError::InvalidValidatorSet))
    );

    client.initialize(&admin, &token, &false, &set, &2);
    assert_eq!(
        client.try_initialize(&admin, &token, &false, &set, &2),
        Err(Ok(BridgeError::AlreadyInitialized))
    );
}

#[test]
fn deposit_locks_funds_in_the_reserve() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let user = Address::generate(&env);
    StellarAssetClient::new(&env, &bridge.token).mint(&user, &300);

    bridge
        .client
        .deposit(&user, &200, &Bytes::from_slice(&env, b"dest-address"));

    assert_eq!(bridge.balance(&user), 100);
    assert_eq!(bridge.balance(&bridge.id), RESERVE + 200);
}

#[test]
fn deposit_burns_in_mint_mode() {
    let env = Env::default();
    let bridge = setup(&env, true, 3, 2);
    let user = Address::generate(&env);
    StellarAssetClient::new(&env, &bridge.token).mint(&user, &300);

    bridge
        .client
        .deposit(&user, &200, &Bytes::from_slice(&env, b"dest-address"));

    assert_eq!(bridge.balance(&user), 100);
    assert_eq!(bridge.balance(&bridge.id), 0);
}

#[test]
fn deposit_rejects_non_positive_amounts() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let user = Address::generate(&env);

    let result = bridge
        .client
        .try_deposit(&user, &0, &Bytes::from_slice(&env, b"dest"));

    assert_eq!(result, Err(Ok(BridgeError::InvalidAmount)));
}

#[test]
fn emergency_shutdown_pauses_deposits_and_withdrawals() {
    let env = Env::default();
    let bridge = setup(&env, false, 3, 2);
    let user = Address::generate(&env);
    let recipient = Address::generate(&env);
    StellarAssetClient::new(&env, &bridge.token).mint(&user, &300);

    bridge.client.set_paused(&true);
    assert!(bridge.client.is_paused());

    let deposit = bridge
        .client
        .try_deposit(&user, &100, &Bytes::from_slice(&env, b"dest"));
    assert_eq!(deposit, Err(Ok(BridgeError::BridgePaused)));

    let sigs = bridge.release_signatures(&[0, 1], &recipient, 100, 1);
    let release = bridge.client.try_release(&recipient, &100, &1, &sigs);
    assert_eq!(release, Err(Ok(BridgeError::BridgePaused)));

    bridge.client.set_paused(&false);
    bridge.client.release(&recipient, &100, &1, &sigs);
    assert_eq!(bridge.balance(&recipient), 100);
}
