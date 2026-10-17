use invariants::{check, Ledger};
use payment_stream::{PaymentStream, PaymentStreamClient};
use proptest::prelude::*;
use soroban_sdk::testutils::{Address as _, Ledger as _};
use soroban_sdk::{Address, Env};

#[derive(Clone, Debug)]
enum Action {
    Advance(u32),
    /// Withdraw this many parts per thousand of the current claimable amount.
    Withdraw(u32),
    Cancel,
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        (0u32..60).prop_map(Action::Advance),
        (0u32..=1_000).prop_map(Action::Withdraw),
        Just(Action::Cancel),
    ]
}

/// The stream never pays out more than it was funded with (solvency), the
/// recipient never receives more than has vested, and balances are conserved.
#[test]
fn payment_stream_is_solvent_and_conserves_supply() {
    check(
        "payment_stream_is_solvent_and_conserves_supply",
        (1i128..1_000_000_000, 1u64..200, prop::collection::vec(action(), 1..16)),
        |(total, duration, actions)| {
            let env = Env::default();
            env.mock_all_auths();
            env.ledger().set_sequence_number(1);

            let contract_id = env.register(PaymentStream, ());
            let client = PaymentStreamClient::new(&env, &contract_id);
            let sender = Address::generate(&env);
            let recipient = Address::generate(&env);

            let mut ledger = Ledger::new(&env);
            ledger.track(&contract_id);
            ledger.track(&sender);
            ledger.track(&recipient);
            ledger.mint(&sender, total);

            let start = 1u64;
            client.initialize(&ledger.asset, &sender, &recipient, &total, &start, &(start + duration));

            for a in actions {
                match a {
                    Action::Advance(n) => {
                        let seq = env.ledger().sequence();
                        env.ledger().set_sequence_number(seq + n);
                    }
                    Action::Withdraw(permille) => {
                        if let Ok(claimable) = client.try_claimable_balance() {
                            let claimable = claimable.unwrap();
                            let amount = claimable * permille as i128 / 1_000;
                            let _ = client.try_withdraw(&amount);
                        }
                    }
                    Action::Cancel => {
                        let _ = client.try_cancel();
                    }
                }

                prop_assert_eq!(ledger.tracked_total(), ledger.minted);
                prop_assert!(ledger.balance(&contract_id) >= 0);
                prop_assert!(ledger.balance(&recipient) <= total);

                let seq = env.ledger().sequence() as u64;
                let vested = if seq <= start {
                    0
                } else if seq >= start + duration {
                    total
                } else {
                    total * (seq - start) as i128 / duration as i128
                };
                // Before cancellation the recipient can never be ahead of
                // the vesting schedule.
                if !client.get_stream().cancelled {
                    prop_assert!(ledger.balance(&recipient) <= vested);
                }
            }
            Ok(())
        },
    );
}
