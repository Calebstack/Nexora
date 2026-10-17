use super::*;
use soroban_sdk::testutils::Address as _;
use soroban_sdk::{vec, Env};

const MAX_BPS: u32 = 2_500;

struct Setup {
    contract: Address,
    asset: Address,
    creator: Address,
    signers: Vec<Address>,
}

fn setup(env: &Env, royalty_bps: u32) -> Setup {
    let admin = Address::generate(env);
    let asset = env.register_stellar_asset_contract_v2(admin).address();
    let creator = Address::generate(env);
    let signers = vec![
        env,
        Address::generate(env),
        Address::generate(env),
        Address::generate(env),
    ];
    let contract = env.register(
        Royalties,
        (
            asset.clone(),
            creator.clone(),
            signers.clone(),
            2u32,
            royalty_bps,
            MAX_BPS,
        ),
    );
    Setup {
        contract,
        asset,
        creator,
        signers,
    }
}

fn mint(env: &Env, asset: &Address, to: &Address, amount: i128) {
    token::StellarAssetClient::new(env, asset).mint(to, &amount);
}

#[test]
fn royalty_info_applies_basis_points_to_sale_price() {
    let env = Env::default();
    let s = setup(&env, 500);
    let client = RoyaltiesClient::new(&env, &s.contract);

    assert_eq!(client.royalty_info(&1_000_000), (s.creator.clone(), 50_000));
    // Rounds down: 5% of 19 is 0.95.
    assert_eq!(client.royalty_info(&19), (s.creator, 0));
}

#[test]
fn royalty_info_rejects_non_positive_price() {
    let env = Env::default();
    let s = setup(&env, 500);
    let client = RoyaltiesClient::new(&env, &s.contract);

    assert_eq!(
        client.try_royalty_info(&0),
        Err(Ok(RoyaltyError::InvalidSalePrice))
    );
    assert_eq!(
        client.try_royalty_info(&-5),
        Err(Ok(RoyaltyError::InvalidSalePrice))
    );
}

#[test]
fn settle_sale_pays_creator_and_seller_atomically() {
    let env = Env::default();
    env.mock_all_auths();
    let s = setup(&env, 1_000);
    let client = RoyaltiesClient::new(&env, &s.contract);
    let buyer = Address::generate(&env);
    let seller = Address::generate(&env);
    mint(&env, &s.asset, &buyer, 1_000);

    assert_eq!(client.settle_sale(&buyer, &seller, &1_000), 100);

    let token = token::Client::new(&env, &s.asset);
    assert_eq!(token.balance(&buyer), 0);
    assert_eq!(token.balance(&s.creator), 100);
    assert_eq!(token.balance(&seller), 900);
}

#[test]
fn settle_sale_reverts_when_buyer_cannot_cover_the_price() {
    let env = Env::default();
    env.mock_all_auths();
    let s = setup(&env, 1_000);
    let client = RoyaltiesClient::new(&env, &s.contract);
    let buyer = Address::generate(&env);
    let seller = Address::generate(&env);
    mint(&env, &s.asset, &buyer, 100);

    assert!(client.try_settle_sale(&buyer, &seller, &1_000).is_err());

    // The royalty transfer that ran first was rolled back with the failure.
    let token = token::Client::new(&env, &s.asset);
    assert_eq!(token.balance(&buyer), 100);
    assert_eq!(token.balance(&s.creator), 0);
    assert_eq!(token.balance(&seller), 0);
}

#[test]
#[should_panic]
fn constructor_rejects_royalty_above_the_maximum() {
    let env = Env::default();
    setup(&env, MAX_BPS + 1);
}

#[test]
fn payout_address_update_needs_threshold_of_signers() {
    let env = Env::default();
    env.mock_all_auths();
    let s = setup(&env, 500);
    let client = RoyaltiesClient::new(&env, &s.contract);
    let new_payout = Address::generate(&env);

    let approvers = vec![&env, s.signers.get(0).unwrap(), s.signers.get(2).unwrap()];
    client.update_payout_address(&new_payout, &approvers);

    assert_eq!(client.get_config().creator, new_payout);
    assert_eq!(client.royalty_info(&10_000).0, new_payout);
}

#[test]
fn payout_address_update_rejects_too_few_approvers() {
    let env = Env::default();
    env.mock_all_auths();
    let s = setup(&env, 500);
    let client = RoyaltiesClient::new(&env, &s.contract);
    let new_payout = Address::generate(&env);

    let approvers = vec![&env, s.signers.get(0).unwrap()];
    assert_eq!(
        client.try_update_payout_address(&new_payout, &approvers),
        Err(Ok(RoyaltyError::InsufficientApprovals))
    );
    assert_eq!(client.get_config().creator, s.creator);
}

#[test]
fn payout_address_update_rejects_duplicate_and_unknown_approvers() {
    let env = Env::default();
    env.mock_all_auths();
    let s = setup(&env, 500);
    let client = RoyaltiesClient::new(&env, &s.contract);
    let new_payout = Address::generate(&env);

    let first = s.signers.get(0).unwrap();
    let duplicated = vec![&env, first.clone(), first];
    assert_eq!(
        client.try_update_payout_address(&new_payout, &duplicated),
        Err(Ok(RoyaltyError::DuplicateApprover))
    );

    let outsider = vec![&env, s.signers.get(1).unwrap(), Address::generate(&env)];
    assert_eq!(
        client.try_update_payout_address(&new_payout, &outsider),
        Err(Ok(RoyaltyError::NotSigner))
    );
}
