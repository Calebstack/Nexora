use super::*;
use soroban_sdk::testutils::Address as _;

fn setup(env: &Env, funded: i128) -> (BatchTransferClient<'_>, Address, Address) {
    let contract_id = env.register(BatchTransfer, ());
    let client = BatchTransferClient::new(env, &contract_id);

    let sender = Address::generate(env);
    let admin = Address::generate(env);
    let asset = env.register_stellar_asset_contract_v2(admin).address();
    token::StellarAssetClient::new(env, &asset).mint(&sender, &funded);

    (client, sender, asset)
}

fn transfer(env: &Env, amount: i128) -> Transfer {
    Transfer {
        to: Address::generate(env),
        amount,
    }
}

#[test]
fn atomic_batch_pays_everyone_and_reports_totals() {
    let env = Env::default();
    env.mock_all_auths();
    let (client, sender, asset) = setup(&env, 1_000);

    let a = transfer(&env, 100);
    let b = transfer(&env, 250);
    let mut batch = Vec::new(&env);
    batch.push_back(a.clone());
    batch.push_back(b.clone());

    let result = client.batch_transfer(&sender, &asset, &batch, &BatchMode::Atomic);

    assert_eq!(result.processed, 2);
    assert_eq!(result.failed, 0);
    assert_eq!(result.total_disbursed, 350);

    let token_client = token::Client::new(&env, &asset);
    assert_eq!(token_client.balance(&a.to), 100);
    assert_eq!(token_client.balance(&b.to), 250);
    assert_eq!(token_client.balance(&sender), 650);
}

#[test]
fn insufficient_total_balance_is_rejected_before_any_transfer() {
    let env = Env::default();
    env.mock_all_auths();
    let (client, sender, asset) = setup(&env, 300);

    let a = transfer(&env, 200);
    let b = transfer(&env, 200);
    let mut batch = Vec::new(&env);
    batch.push_back(a.clone());
    batch.push_back(b);

    let result = client.try_batch_transfer(&sender, &asset, &batch, &BatchMode::BestEffort);
    assert_eq!(result, Err(Ok(BatchError::InsufficientBalance)));

    let token_client = token::Client::new(&env, &asset);
    assert_eq!(token_client.balance(&a.to), 0);
    assert_eq!(token_client.balance(&sender), 300);
}

#[test]
fn atomic_mode_rejects_a_batch_containing_an_invalid_amount() {
    let env = Env::default();
    env.mock_all_auths();
    let (client, sender, asset) = setup(&env, 1_000);

    let good = transfer(&env, 100);
    let mut batch = Vec::new(&env);
    batch.push_back(good.clone());
    batch.push_back(transfer(&env, 0));

    let result = client.try_batch_transfer(&sender, &asset, &batch, &BatchMode::Atomic);
    assert_eq!(result, Err(Ok(BatchError::InvalidAmount)));

    let token_client = token::Client::new(&env, &asset);
    assert_eq!(token_client.balance(&good.to), 0);
}

#[test]
fn best_effort_mode_skips_invalid_entries_and_pays_the_rest() {
    let env = Env::default();
    env.mock_all_auths();
    let (client, sender, asset) = setup(&env, 1_000);

    let good = transfer(&env, 100);
    let mut batch = Vec::new(&env);
    batch.push_back(transfer(&env, -5));
    batch.push_back(good.clone());

    let result = client.batch_transfer(&sender, &asset, &batch, &BatchMode::BestEffort);

    assert_eq!(result.processed, 1);
    assert_eq!(result.failed, 1);
    assert_eq!(result.total_disbursed, 100);
    assert_eq!(token::Client::new(&env, &asset).balance(&good.to), 100);
}

#[test]
fn batch_size_limits_are_enforced() {
    let env = Env::default();
    env.mock_all_auths();
    let (client, sender, asset) = setup(&env, 1_000_000);

    let empty = Vec::new(&env);
    assert_eq!(
        client.try_batch_transfer(&sender, &asset, &empty, &BatchMode::Atomic),
        Err(Ok(BatchError::EmptyBatch))
    );

    let mut at_limit = Vec::new(&env);
    for _ in 0..MAX_TRANSFERS {
        at_limit.push_back(transfer(&env, 1));
    }
    let result = client.batch_transfer(&sender, &asset, &at_limit, &BatchMode::Atomic);
    assert_eq!(result.processed, MAX_TRANSFERS);

    let mut over_limit = at_limit.clone();
    over_limit.push_back(transfer(&env, 1));
    assert_eq!(
        client.try_batch_transfer(&sender, &asset, &over_limit, &BatchMode::Atomic),
        Err(Ok(BatchError::TooManyTransfers))
    );
}
