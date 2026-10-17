use super::*;
use soroban_sdk::{
    testutils::{Address as _, Ledger as _},
    Address, BytesN, Env, String, Vec,
};

pub(crate) const DAY: u64 = 20_000;

pub(crate) fn setup(env: &Env) -> (AttestationContractClient<'_>, Address) {
    env.mock_all_auths();
    env.ledger().with_mut(|li| li.timestamp = (DAY + 1) * SECONDS_PER_DAY);

    let contract_id = env.register(AttestationContract, ());
    let client = AttestationContractClient::new(env, &contract_id);
    let admin = Address::generate(env);
    let maintainer = Address::generate(env);

    client.initialize(&admin);
    client.add_maintainer(&maintainer);

    (client, maintainer)
}

fn leaf(env: &Env, n: u8) -> BytesN<32> {
    BytesN::from_array(env, &[n; 32])
}

#[test]
fn publish_and_read_attestation() {
    let env = Env::default();
    let (client, maintainer) = setup(&env);
    let root = leaf(&env, 9);
    let metadata = String::from_str(&env, "{\"events\":4}");

    client.publish_attestation(&maintainer, &DAY, &root, &metadata);

    let record = client.get_attestation(&DAY);
    assert_eq!(record.merkle_root, root);
    assert_eq!(record.published_at, (DAY + 1) * SECONDS_PER_DAY);
    assert_eq!(record.published_by, maintainer);
    assert_eq!(record.metadata, metadata);
    assert_eq!(env.events().all().len(), 1);
}

#[test]
fn attestation_cannot_be_overwritten() {
    let env = Env::default();
    let (client, maintainer) = setup(&env);
    let metadata = String::from_str(&env, "first");

    client.publish_attestation(&maintainer, &DAY, &leaf(&env, 1), &metadata);
    let second = client.try_publish_attestation(&maintainer, &DAY, &leaf(&env, 2), &metadata);

    assert_eq!(second, Err(Ok(AttestationError::AlreadyAttested)));
    assert_eq!(client.get_attestation(&DAY).merkle_root, leaf(&env, 1));
}

#[test]
fn only_maintainers_can_publish() {
    let env = Env::default();
    let (client, _) = setup(&env);
    let outsider = Address::generate(&env);

    let result = client.try_publish_attestation(
        &outsider,
        &DAY,
        &leaf(&env, 1),
        &String::from_str(&env, "x"),
    );

    assert_eq!(result, Err(Ok(AttestationError::NotMaintainer)));
}

#[test]
fn removed_maintainer_loses_access() {
    let env = Env::default();
    let (client, maintainer) = setup(&env);

    client.remove_maintainer(&maintainer);

    let result = client.try_publish_attestation(
        &maintainer,
        &DAY,
        &leaf(&env, 1),
        &String::from_str(&env, "x"),
    );
    assert_eq!(result, Err(Ok(AttestationError::NotMaintainer)));
}

#[test]
fn future_days_are_rejected() {
    let env = Env::default();
    let (client, maintainer) = setup(&env);

    let result = client.try_publish_attestation(
        &maintainer,
        &(DAY + 2),
        &leaf(&env, 1),
        &String::from_str(&env, "x"),
    );

    assert_eq!(result, Err(Ok(AttestationError::FutureDay)));
}

#[test]
fn initialize_only_once() {
    let env = Env::default();
    let (client, _) = setup(&env);

    let result = client.try_initialize(&Address::generate(&env));

    assert_eq!(result, Err(Ok(AttestationError::AlreadyInitialized)));
}

#[test]
fn missing_attestation_is_not_found() {
    let env = Env::default();
    let (client, _) = setup(&env);

    assert_eq!(
        client.try_get_attestation(&DAY),
        Err(Ok(AttestationError::NotFound))
    );
}

#[test]
fn verify_inclusion_accepts_valid_proofs_only() {
    let env = Env::default();
    let (client, maintainer) = setup(&env);

    // Four leaves: root = H(H(l0,l1), H(l2,l3)).
    let leaves = [leaf(&env, 1), leaf(&env, 2), leaf(&env, 3), leaf(&env, 4)];
    let n01 = hash_pair(&env, &leaves[0], &leaves[1]);
    let n23 = hash_pair(&env, &leaves[2], &leaves[3]);
    let root = hash_pair(&env, &n01, &n23);
    client.publish_attestation(&maintainer, &DAY, &root, &String::from_str(&env, "x"));

    let mut proof = Vec::new(&env);
    proof.push_back(leaves[3].clone());
    proof.push_back(n01.clone());
    assert!(client.verify_inclusion(&DAY, &leaves[2], &proof));

    let mut proof = Vec::new(&env);
    proof.push_back(leaves[1].clone());
    proof.push_back(n23.clone());
    assert!(client.verify_inclusion(&DAY, &leaves[0], &proof));

    // A leaf that is not in the tree, and a proof for the wrong leaf, fail.
    assert!(!client.verify_inclusion(&DAY, &leaf(&env, 7), &proof));
    assert!(!client.verify_inclusion(&DAY, &leaves[1], &proof));
}

#[test]
fn verify_inclusion_requires_an_attestation() {
    let env = Env::default();
    let (client, _) = setup(&env);

    let result = client.try_verify_inclusion(&DAY, &leaf(&env, 1), &Vec::new(&env));

    assert_eq!(result, Err(Ok(AttestationError::NotFound)));
}
