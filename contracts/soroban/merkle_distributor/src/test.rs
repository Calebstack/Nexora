use super::*;
use soroban_sdk::testutils::{Address as _, Ledger};
use soroban_sdk::{vec, Env};

const EXPIRATION: u32 = 100;

struct Airdrop {
    contract: Address,
    asset: Address,
    sender: Address,
    recipients: [(u32, Address, i128); 3],
    proofs: [Vec<BytesN<32>>; 3],
}

/// Three leaves: root = pair(pair(l0, l1), l2), so index 2 is the odd leaf
/// promoted to the next level.
fn setup(env: &Env) -> Airdrop {
    let admin = Address::generate(env);
    let asset = env.register_stellar_asset_contract_v2(admin).address();
    let sender = Address::generate(env);
    let recipients = [
        (0u32, Address::generate(env), 100i128),
        (1u32, Address::generate(env), 250i128),
        (2u32, Address::generate(env), 400i128),
    ];

    let l0 = leaf_hash(env, recipients[0].0, &recipients[0].1, recipients[0].2);
    let l1 = leaf_hash(env, recipients[1].0, &recipients[1].1, recipients[1].2);
    let l2 = leaf_hash(env, recipients[2].0, &recipients[2].1, recipients[2].2);
    let p01 = hash_pair(env, &l0, &l1);
    let root = hash_pair(env, &p01, &l2);

    let contract = env.register(
        MerkleDistributor,
        (root, asset.clone(), sender.clone(), EXPIRATION),
    );
    token::StellarAssetClient::new(env, &asset).mint(&contract, &750);

    Airdrop {
        contract,
        asset,
        sender,
        recipients,
        proofs: [
            vec![env, l1.clone(), l2.clone()],
            vec![env, l0, l2],
            vec![env, p01],
        ],
    }
}

#[test]
fn valid_proof_pays_recipient_and_marks_index_claimed() {
    let env = Env::default();
    env.mock_all_auths();
    let a = setup(&env);
    let client = MerkleDistributorClient::new(&env, &a.contract);

    for i in 0..3 {
        let (index, recipient, amount) = a.recipients[i].clone();
        assert!(!client.is_claimed(&index));
        client.claim(&index, &recipient, &amount, &a.proofs[i]);
        assert!(client.is_claimed(&index));
        assert_eq!(token::Client::new(&env, &a.asset).balance(&recipient), amount);
    }
}

#[test]
fn second_claim_for_the_same_index_is_rejected() {
    let env = Env::default();
    env.mock_all_auths();
    let a = setup(&env);
    let client = MerkleDistributorClient::new(&env, &a.contract);
    let (index, recipient, amount) = a.recipients[0].clone();

    client.claim(&index, &recipient, &amount, &a.proofs[0]);
    assert_eq!(
        client.try_claim(&index, &recipient, &amount, &a.proofs[0]),
        Err(Ok(DistributorError::AlreadyClaimed))
    );
}

#[test]
fn tampered_amount_recipient_or_index_is_rejected() {
    let env = Env::default();
    env.mock_all_auths();
    let a = setup(&env);
    let client = MerkleDistributorClient::new(&env, &a.contract);
    let (index, recipient, amount) = a.recipients[0].clone();

    assert_eq!(
        client.try_claim(&index, &recipient, &(amount + 1), &a.proofs[0]),
        Err(Ok(DistributorError::InvalidProof))
    );
    assert_eq!(
        client.try_claim(&index, &Address::generate(&env), &amount, &a.proofs[0]),
        Err(Ok(DistributorError::InvalidProof))
    );
    assert_eq!(
        client.try_claim(&1, &recipient, &amount, &a.proofs[0]),
        Err(Ok(DistributorError::InvalidProof))
    );
    assert!(!client.is_claimed(&index));
}

#[test]
fn claim_requires_a_positive_amount() {
    let env = Env::default();
    env.mock_all_auths();
    let a = setup(&env);
    let client = MerkleDistributorClient::new(&env, &a.contract);
    let (index, recipient, _) = a.recipients[0].clone();

    assert_eq!(
        client.try_claim(&index, &recipient, &0, &a.proofs[0]),
        Err(Ok(DistributorError::InvalidAmount))
    );
}

#[test]
fn claims_close_after_the_expiration_ledger() {
    let env = Env::default();
    env.mock_all_auths();
    let a = setup(&env);
    let client = MerkleDistributorClient::new(&env, &a.contract);
    let (index, recipient, amount) = a.recipients[0].clone();

    env.ledger().with_mut(|l| l.sequence_number = EXPIRATION + 1);
    assert_eq!(
        client.try_claim(&index, &recipient, &amount, &a.proofs[0]),
        Err(Ok(DistributorError::ClaimPeriodEnded))
    );
}

#[test]
fn sender_can_claw_back_only_after_expiration() {
    let env = Env::default();
    env.mock_all_auths();
    let a = setup(&env);
    let client = MerkleDistributorClient::new(&env, &a.contract);
    let (index, recipient, amount) = a.recipients[0].clone();
    client.claim(&index, &recipient, &amount, &a.proofs[0]);

    assert_eq!(
        client.try_clawback(),
        Err(Ok(DistributorError::ClaimPeriodActive))
    );

    env.ledger().with_mut(|l| l.sequence_number = EXPIRATION + 1);
    assert_eq!(client.clawback(), 650);
    assert_eq!(token::Client::new(&env, &a.asset).balance(&a.sender), 650);
    assert_eq!(token::Client::new(&env, &a.asset).balance(&a.contract), 0);
}

#[test]
fn bitmap_packs_128_indices_per_word() {
    let env = Env::default();
    env.mock_all_auths();
    let a = setup(&env);

    env.as_contract(&a.contract, || {
        MerkleDistributor::set_claimed(&env, 127);
        MerkleDistributor::set_claimed(&env, 128);

        assert!(MerkleDistributor::claimed(&env, 127));
        assert!(MerkleDistributor::claimed(&env, 128));
        assert!(!MerkleDistributor::claimed(&env, 126));
        assert!(!MerkleDistributor::claimed(&env, 129));
        assert!(env.storage().persistent().has(&DataKey::ClaimedWord(0)));
        assert!(env.storage().persistent().has(&DataKey::ClaimedWord(1)));
        assert!(!env.storage().persistent().has(&DataKey::ClaimedWord(2)));
    });
}

#[test]
#[should_panic]
fn constructor_rejects_an_expiration_in_the_past() {
    let env = Env::default();
    env.ledger().with_mut(|l| l.sequence_number = 50);
    let asset = Address::generate(&env);
    let sender = Address::generate(&env);
    env.register(
        MerkleDistributor,
        (BytesN::from_array(&env, &[0u8; 32]), asset, sender, 50u32),
    );
}
