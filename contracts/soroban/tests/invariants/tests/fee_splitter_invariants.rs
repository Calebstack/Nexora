use fee_splitter::{Beneficiary, FeeSplitter, FeeSplitterClient};
use invariants::{check, Ledger};
use proptest::prelude::*;
use soroban_sdk::testutils::Address as _;
use soroban_sdk::{Address, Env, Vec};

/// Splits `total` basis points (10_000) into `n` positive parts.
fn bps_split() -> impl Strategy<Value = std::vec::Vec<u32>> {
    prop::collection::vec(1u32..1_000, 1..8).prop_map(|weights| {
        let sum: u32 = weights.iter().sum();
        let mut parts: std::vec::Vec<u32> = weights.iter().map(|w| w * 10_000 / sum).collect();
        let assigned: u32 = parts.iter().sum();
        let last = parts.len() - 1;
        parts[last] += 10_000 - assigned;
        parts
    })
}

/// Every split pays out exactly the amount funded: no token is created,
/// destroyed or stranded in the splitter, and rounding dust goes to the
/// primary recipient.
#[test]
fn fee_splitter_distributes_exactly_the_fee() {
    check(
        "fee_splitter_distributes_exactly_the_fee",
        (bps_split(), 1i128..1_000_000_000_000),
        |(parts, amount)| {
            let env = Env::default();
            env.mock_all_auths();

            let contract_id = env.register(FeeSplitter, ());
            let client = FeeSplitterClient::new(&env, &contract_id);
            let admin = Address::generate(&env);
            let primary = Address::generate(&env);

            let mut ledger = Ledger::new(&env);
            ledger.track(&contract_id);
            ledger.track(&primary);

            let mut beneficiaries = Vec::new(&env);
            let mut addresses = std::vec::Vec::new();
            for bps in parts.iter() {
                let address = Address::generate(&env);
                ledger.track(&address);
                addresses.push(address.clone());
                beneficiaries.push_back(Beneficiary { address, basis_points: *bps });
            }

            client.initialize(&ledger.asset, &admin, &beneficiaries, &primary);
            ledger.mint(&contract_id, amount);
            client.split_fee(&amount);

            prop_assert_eq!(ledger.balance(&contract_id), 0);
            prop_assert_eq!(ledger.tracked_total(), ledger.minted);

            let mut expected_paid = 0i128;
            for (address, bps) in addresses.iter().zip(parts.iter()) {
                let share = amount * (*bps as i128) / 10_000;
                prop_assert_eq!(ledger.balance(address), share);
                expected_paid += share;
            }
            prop_assert_eq!(ledger.balance(&primary), amount - expected_paid);
            Ok(())
        },
    );
}
