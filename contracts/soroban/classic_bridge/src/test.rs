use super::*;
use soroban_sdk::testutils::Address as _;

struct Fixture<'a> {
    env: Env,
    client: ClassicBridgeClient<'a>,
    pool_id: BytesN<32>,
    asset_a: Address,
    asset_b: Address,
    lp: Address,
}

fn setup<'a>() -> Fixture<'a> {
    let env = Env::default();
    env.mock_all_auths();

    let contract_id = env.register(ClassicBridge, ());
    let client = ClassicBridgeClient::new(&env, &contract_id);
    client.initialize(&Address::generate(&env));

    let asset_a = env.register_stellar_asset_contract_v2(Address::generate(&env)).address();
    let asset_b = env.register_stellar_asset_contract_v2(Address::generate(&env)).address();
    let pool_id = BytesN::from_array(&env, &[7u8; 32]);
    client.register_pool(&pool_id, &asset_a, &asset_b, &30);

    let lp = Address::generate(&env);
    token::StellarAssetClient::new(&env, &asset_a).mint(&lp, &1_000_000);
    token::StellarAssetClient::new(&env, &asset_b).mint(&lp, &1_000_000);

    Fixture { env, client, pool_id, asset_a, asset_b, lp }
}

#[test]
fn first_deposit_sets_the_price_and_wraps_shares() {
    let f = setup();
    let shares = f.client.deposit(&f.pool_id, &f.lp, &10_000, &40_000, &0);

    assert_eq!(shares, 20_000);
    assert_eq!(f.client.shares_of(&f.pool_id, &f.lp), 20_000);
    let pool = f.client.get_pool(&f.pool_id);
    assert_eq!((pool.reserve_a, pool.reserve_b, pool.total_shares), (10_000, 40_000, 20_000));
}

#[test]
fn withdraw_redeems_the_underlying_tokens() {
    let f = setup();
    f.client.deposit(&f.pool_id, &f.lp, &10_000, &40_000, &0);

    let (a, b) = f.client.withdraw(&f.pool_id, &f.lp, &10_000, &0, &0);
    assert_eq!((a, b), (5_000, 20_000));
    assert_eq!(token::Client::new(&f.env, &f.asset_a).balance(&f.lp), 995_000);

    assert_eq!(
        f.client.try_withdraw(&f.pool_id, &f.lp, &10_001, &0, &0),
        Err(Ok(BridgeError::InsufficientShares))
    );
}

#[test]
fn swap_follows_constant_product_and_never_shrinks_k() {
    let f = setup();
    f.client.deposit(&f.pool_id, &f.lp, &100_000, &100_000, &0);

    let trader = Address::generate(&f.env);
    token::StellarAssetClient::new(&f.env, &f.asset_a).mint(&trader, &1_000);

    let quoted = f.client.quote(&f.pool_id, &f.asset_a, &1_000);
    assert!(f.client.verify_trade(&f.pool_id, &f.asset_a, &1_000, &quoted));
    assert!(!f.client.verify_trade(&f.pool_id, &f.asset_a, &1_000, &(quoted + 1)));

    let out = f.client.swap(&f.pool_id, &trader, &f.asset_a, &1_000, &quoted);
    assert_eq!(out, quoted);

    let pool = f.client.get_pool(&f.pool_id);
    assert!(pool.reserve_a * pool.reserve_b >= 100_000i128 * 100_000i128);
    assert_eq!(token::Client::new(&f.env, &f.asset_b).balance(&trader), out);
}

#[test]
fn swap_rejects_slippage_and_unknown_assets() {
    let f = setup();
    f.client.deposit(&f.pool_id, &f.lp, &100_000, &100_000, &0);

    let trader = Address::generate(&f.env);
    token::StellarAssetClient::new(&f.env, &f.asset_a).mint(&trader, &1_000);

    let quoted = f.client.quote(&f.pool_id, &f.asset_a, &1_000);
    assert_eq!(
        f.client.try_swap(&f.pool_id, &trader, &f.asset_a, &1_000, &(quoted + 1)),
        Err(Ok(BridgeError::SlippageExceeded))
    );

    let stranger = Address::generate(&f.env);
    assert_eq!(
        f.client.try_swap(&f.pool_id, &trader, &stranger, &1_000, &0),
        Err(Ok(BridgeError::InvalidAsset))
    );
}

#[test]
fn deposit_enforces_min_shares_and_pool_must_have_liquidity_to_quote() {
    let f = setup();
    assert_eq!(
        f.client.try_quote(&f.pool_id, &f.asset_a, &100),
        Err(Ok(BridgeError::InsufficientLiquidity))
    );

    f.client.deposit(&f.pool_id, &f.lp, &10_000, &10_000, &0);
    // A lopsided deposit only earns shares for the smaller side.
    assert_eq!(
        f.client.try_deposit(&f.pool_id, &f.lp, &1_000, &10_000, &5_000),
        Err(Ok(BridgeError::SlippageExceeded))
    );
}

#[test]
fn sync_reserves_updates_the_mirror() {
    let f = setup();
    f.client.sync_reserves(&f.pool_id, &500, &800, &600);
    let pool = f.client.get_pool(&f.pool_id);
    assert_eq!((pool.reserve_a, pool.reserve_b, pool.total_shares), (500, 800, 600));
}
