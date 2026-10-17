use super::*;
use soroban_sdk::testutils::{Address as _, Ledger};
use soroban_sdk::{vec, IntoVal};

const PER_TX: i128 = 100;
const DAILY: i128 = 250;
const DELAY: u64 = 1_000;

struct Fixture<'a> {
    env: Env,
    client: SmartWalletClient<'a>,
    owner: Address,
    guardians: [Address; 3],
    asset: Address,
}

fn setup<'a>() -> Fixture<'a> {
    let env = Env::default();
    env.mock_all_auths();

    let owner = Address::generate(&env);
    let guardians = [
        Address::generate(&env),
        Address::generate(&env),
        Address::generate(&env),
    ];
    let mut list = Vec::new(&env);
    for g in guardians.iter() {
        list.push_back(g.clone());
    }

    let contract_id = env.register(SmartWallet, (owner.clone(), list, 2u32, PER_TX, DAILY, DELAY));
    let client = SmartWalletClient::new(&env, &contract_id);

    let asset = env
        .register_stellar_asset_contract_v2(Address::generate(&env))
        .address();
    token::StellarAssetClient::new(&env, &asset).mint(&contract_id, &10_000);

    Fixture { env, client, owner, guardians, asset }
}

#[test]
fn owner_can_invoke_arbitrary_contracts_within_caps() {
    let f = setup();
    let to = Address::generate(&f.env);

    let args: Vec<Val> = vec![
        &f.env,
        f.client.address.clone().into_val(&f.env),
        to.clone().into_val(&f.env),
        (40i128).into_val(&f.env),
    ];
    f.client
        .execute(&f.asset, &Symbol::new(&f.env, "transfer"), &args, &40, &0);

    assert_eq!(token::Client::new(&f.env, &f.asset).balance(&to), 40);
    assert_eq!(f.client.get_state().spent_today, 40);
}

#[test]
fn per_transaction_and_daily_caps_are_enforced() {
    let f = setup();
    let key = Address::generate(&f.env);
    let to = Address::generate(&f.env);
    let now = f.env.ledger().timestamp();
    f.client.add_session_key(&key, &(now + 10_000));

    assert_eq!(
        f.client.try_session_transfer(&key, &f.asset, &to, &(PER_TX + 1), &0),
        Err(Ok(WalletError::PerTxCapExceeded))
    );

    f.client.session_transfer(&key, &f.asset, &to, &100, &0);
    f.client.session_transfer(&key, &f.asset, &to, &100, &1);
    // 200 spent, a further 100 would reach 300 > 250.
    assert_eq!(
        f.client.try_session_transfer(&key, &f.asset, &to, &100, &2),
        Err(Ok(WalletError::DailyCapExceeded))
    );

    // The daily window rolls over after 24 hours.
    f.env.ledger().set_timestamp(now + DAY_SECONDS + 1);
    f.client.add_session_key(&key, &(now + 2 * DAY_SECONDS));
    f.client.session_transfer(&key, &f.asset, &to, &100, &2);
}

#[test]
fn replayed_nonce_is_rejected() {
    let f = setup();
    let key = Address::generate(&f.env);
    let to = Address::generate(&f.env);
    f.client.add_session_key(&key, &10_000);

    f.client.session_transfer(&key, &f.asset, &to, &10, &0);
    assert_eq!(
        f.client.try_session_transfer(&key, &f.asset, &to, &10, &0),
        Err(Ok(WalletError::InvalidNonce))
    );
}

#[test]
fn expired_and_revoked_session_keys_cannot_spend() {
    let f = setup();
    let key = Address::generate(&f.env);
    let to = Address::generate(&f.env);
    f.client.add_session_key(&key, &500);

    f.env.ledger().set_timestamp(500);
    assert_eq!(
        f.client.try_session_transfer(&key, &f.asset, &to, &10, &0),
        Err(Ok(WalletError::SessionExpired))
    );

    f.client.add_session_key(&key, &10_000);
    f.client.revoke_session_key(&key);
    assert_eq!(
        f.client.try_session_transfer(&key, &f.asset, &to, &10, &0),
        Err(Ok(WalletError::SessionNotFound))
    );
}

#[test]
fn recovery_rotates_the_owner_after_threshold_guardians_approve() {
    let f = setup();
    let new_owner = Address::generate(&f.env);

    let id = f.client.propose_recovery(&f.guardians[0], &new_owner);
    assert_eq!(f.client.get_state().owner, f.owner);

    f.client.approve_recovery(&f.guardians[1], &id);
    assert_eq!(f.client.get_state().owner, new_owner);

    // The proposal is gone, so approving it again cannot replay the rotation.
    assert_eq!(
        f.client.try_approve_recovery(&f.guardians[2], &id),
        Err(Ok(WalletError::NoPendingRecovery))
    );
}

#[test]
fn unauthorized_recovery_attempts_are_rejected() {
    let f = setup();
    let attacker = Address::generate(&f.env);

    assert_eq!(
        f.client.try_propose_recovery(&attacker, &attacker),
        Err(Ok(WalletError::NotGuardian))
    );

    let id = f.client.propose_recovery(&f.guardians[0], &attacker);
    assert_eq!(
        f.client.try_approve_recovery(&attacker, &id),
        Err(Ok(WalletError::NotGuardian))
    );
    // The same guardian cannot approve twice to reach the threshold alone.
    assert_eq!(
        f.client.try_approve_recovery(&f.guardians[0], &id),
        Err(Ok(WalletError::AlreadyApproved))
    );
    assert_eq!(f.client.get_state().owner, f.owner);
}

#[test]
fn cap_changes_are_time_delayed() {
    let f = setup();
    let now = f.env.ledger().timestamp();

    f.client.propose_caps(&500, &1_000);
    assert_eq!(f.client.get_state().per_tx_cap, PER_TX);
    assert_eq!(
        f.client.try_apply_caps(),
        Err(Ok(WalletError::DelayNotElapsed))
    );

    f.env.ledger().set_timestamp(now + DELAY);
    f.client.apply_caps();

    let state = f.client.get_state();
    assert_eq!(state.per_tx_cap, 500);
    assert_eq!(state.daily_cap, 1_000);
    assert_eq!(f.client.try_apply_caps(), Err(Ok(WalletError::NoPendingCaps)));
}
