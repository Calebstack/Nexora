use escrow::{Escrow, EscrowClient};
use invariants::{check, Ledger};
use proptest::prelude::*;
use soroban_sdk::testutils::{Address as _, Ledger as _};
use soroban_sdk::{Address, Env};

const DEADLINE: u64 = 100;

#[derive(Clone, Debug)]
enum Action {
    Sign(u8),
    Advance(u32),
    Refund,
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        (0u8..6).prop_map(Action::Sign),
        (0u32..80).prop_map(Action::Advance),
        Just(Action::Refund),
    ]
}

/// Balances never leave the closed system, the escrow holds either the full
/// amount or nothing, and funds are paid out at most once (released to the
/// recipient or refunded to the sender, never both).
#[test]
fn escrow_conserves_supply_and_pays_out_once() {
    check(
        "escrow_conserves_supply_and_pays_out_once",
        (1i128..1_000_000_000, 1u32..=4, 0u32..=2, prop::collection::vec(action(), 1..16)),
        |(amount, threshold, extra_signers, actions)| {
            let env = Env::default();
            env.mock_all_auths();

            let contract_id = env.register(Escrow, ());
            let client = EscrowClient::new(&env, &contract_id);
            let sender = Address::generate(&env);
            let recipient = Address::generate(&env);
            let signers: std::vec::Vec<Address> =
                (0..6).map(|_| Address::generate(&env)).collect();

            let mut ledger = Ledger::new(&env);
            ledger.track(&contract_id);
            ledger.track(&sender);
            ledger.track(&recipient);
            ledger.mint(&sender, amount);

            client.initialize(
                &ledger.asset,
                &sender,
                &recipient,
                &amount,
                &threshold,
                &(threshold + extra_signers),
                &DEADLINE,
            );

            for a in actions {
                match a {
                    Action::Sign(i) => {
                        let _ = client.try_sign(&signers[i as usize]);
                    }
                    Action::Advance(n) => {
                        let seq = env.ledger().sequence();
                        env.ledger().set_sequence_number(seq + n);
                    }
                    Action::Refund => {
                        let _ = client.try_refund();
                    }
                }

                prop_assert_eq!(ledger.tracked_total(), ledger.minted);

                let held = ledger.balance(&contract_id);
                let paid = ledger.balance(&recipient);
                let refunded = ledger.balance(&sender);
                prop_assert!(held == 0 || held == amount);
                prop_assert!(paid == 0 || paid == amount);
                if held == amount {
                    prop_assert_eq!(paid, 0);
                    prop_assert_eq!(refunded, 0);
                } else {
                    // Exactly one of release or refund happened.
                    prop_assert_eq!(paid + refunded, amount);
                }
            }
            Ok(())
        },
    );
}
