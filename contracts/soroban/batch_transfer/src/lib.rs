#![no_std]

use soroban_sdk::{
    contract, contracterror, contractimpl, contracttype, symbol_short, token, Address, Env, Vec,
};

/// Soroban transaction limits keep a single invocation to this many
/// sub-transfers.
pub const MAX_TRANSFERS: u32 = 100;

#[derive(Clone)]
#[contracttype]
pub struct Transfer {
    pub to: Address,
    pub amount: i128,
}

/// `Atomic` reverts the whole batch if any transfer is invalid or fails.
/// `BestEffort` skips failing transfers, logs each one as an event and
/// disburses the rest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[contracttype]
pub enum BatchMode {
    Atomic,
    BestEffort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[contracttype]
pub struct BatchResult {
    pub processed: u32,
    pub failed: u32,
    pub total_disbursed: i128,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum BatchError {
    EmptyBatch = 1,
    TooManyTransfers = 2,
    InvalidAmount = 3,
    InsufficientBalance = 4,
    Overflow = 5,
}

#[contract]
pub struct BatchTransfer;

#[contractimpl]
impl BatchTransfer {
    /// Pays every entry of `transfers` from `sender` in `asset`.
    ///
    /// The sender's balance is checked against the sum of all amounts before
    /// any sub-transfer runs. In `Atomic` mode a failing sub-transfer traps
    /// and the host rolls the whole invocation back.
    pub fn batch_transfer(
        env: Env,
        sender: Address,
        asset: Address,
        transfers: Vec<Transfer>,
        mode: BatchMode,
    ) -> Result<BatchResult, BatchError> {
        sender.require_auth();

        if transfers.is_empty() {
            return Err(BatchError::EmptyBatch);
        }
        if transfers.len() > MAX_TRANSFERS {
            return Err(BatchError::TooManyTransfers);
        }

        let mut total: i128 = 0;
        for t in transfers.iter() {
            if t.amount <= 0 {
                if mode == BatchMode::Atomic {
                    return Err(BatchError::InvalidAmount);
                }
                continue;
            }
            total = total.checked_add(t.amount).ok_or(BatchError::Overflow)?;
        }

        let token_client = token::Client::new(&env, &asset);
        if token_client.balance(&sender) < total {
            return Err(BatchError::InsufficientBalance);
        }

        let mut processed: u32 = 0;
        let mut failed: u32 = 0;
        let mut disbursed: i128 = 0;

        for (index, t) in transfers.iter().enumerate() {
            let ok = if t.amount <= 0 {
                false
            } else if mode == BatchMode::Atomic {
                token_client.transfer(&sender, &t.to, &t.amount);
                true
            } else {
                matches!(token_client.try_transfer(&sender, &t.to, &t.amount), Ok(Ok(())))
            };

            if ok {
                processed += 1;
                disbursed += t.amount;
            } else {
                failed += 1;
                env.events().publish(
                    (symbol_short!("batch"), symbol_short!("failed"), index as u32),
                    (t.to.clone(), t.amount),
                );
            }
        }

        env.events().publish(
            (symbol_short!("batch"), symbol_short!("done")),
            (processed, failed, disbursed),
        );

        Ok(BatchResult {
            processed,
            failed,
            total_disbursed: disbursed,
        })
    }
}

#[cfg(test)]
mod test;
