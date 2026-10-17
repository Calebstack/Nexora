use batch_transfer::{BatchMode, BatchTransfer, BatchTransferClient, Transfer};
use invariants::{check, Ledger};
use proptest::prelude::*;
use soroban_sdk::testutils::Address as _;
use soroban_sdk::{Address, Env, Vec};

/// Atomic batches either pay everything or nothing; best-effort batches pay
/// exactly the valid entries. Either way the total supply is conserved and
/// the reported disbursement matches the balances.
#[test]
fn batch_transfer_conserves_supply_in_both_modes() {
    check(
        "batch_transfer_conserves_supply_in_both_modes",
        (
            prop::collection::vec(-5i128..1_000, 1..30),
            1i128..40_000,
            any::<bool>(),
        ),
        |(amounts, funded, atomic)| {
            let env = Env::default();
            env.mock_all_auths();

            let contract_id = env.register(BatchTransfer, ());
            let client = BatchTransferClient::new(&env, &contract_id);
            let sender = Address::generate(&env);

            let mut ledger = Ledger::new(&env);
            ledger.track(&sender);
            ledger.mint(&sender, funded);

            let mut batch = Vec::new(&env);
            let mut recipients = std::vec::Vec::new();
            for amount in amounts.iter() {
                let to = Address::generate(&env);
                ledger.track(&to);
                recipients.push(to.clone());
                batch.push_back(Transfer { to, amount: *amount });
            }

            let mode = if atomic { BatchMode::Atomic } else { BatchMode::BestEffort };
            let valid_total: i128 = amounts.iter().filter(|a| **a > 0).sum();
            let result = client.try_batch_transfer(&sender, &ledger.asset, &batch, &mode);

            prop_assert_eq!(ledger.tracked_total(), ledger.minted);

            match result {
                Ok(Ok(r)) => {
                    prop_assert!(valid_total <= funded);
                    prop_assert_eq!(r.total_disbursed, valid_total);
                    prop_assert_eq!(ledger.balance(&sender), funded - valid_total);
                    if atomic {
                        prop_assert_eq!(r.failed, 0);
                    }
                }
                _ => {
                    // A rejected batch moves nothing.
                    prop_assert_eq!(ledger.balance(&sender), funded);
                }
            }
            Ok(())
        },
    );
}
