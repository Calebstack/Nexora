use soroban_sdk::{contracterror, contracttype, panic_with_error, Env};

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum GuardError {
    Reentrancy = 1000,
}

#[derive(Clone)]
#[contracttype]
pub enum GuardKey {
    Entered,
}

/// Marks the current contract as executing and clears the mark when dropped.
///
/// The mark lives in temporary storage, so it never adds rent-bearing state:
/// the only cost of a call that is not re-entered is one temporary entry
/// written on entry and removed on exit. A second `enter` while the guard is
/// alive means a cross-contract call came back into the same contract, and it
/// reverts the whole invocation.
pub struct ReentrancyGuard {
    env: Env,
}

impl ReentrancyGuard {
    /// Enters the guarded section, returning `GuardError::Reentrancy` if it is
    /// already active so callers can map the failure onto their own error enum.
    pub fn try_enter(env: &Env) -> Result<Self, GuardError> {
        let storage = env.storage().temporary();
        if storage.has(&GuardKey::Entered) {
            return Err(GuardError::Reentrancy);
        }
        storage.set(&GuardKey::Entered, &true);
        Ok(Self { env: env.clone() })
    }

    /// Enters the guarded section and reverts the transaction on re-entry.
    pub fn enter(env: &Env) -> Self {
        match Self::try_enter(env) {
            Ok(guard) => guard,
            Err(err) => panic_with_error!(env, err),
        }
    }
}

impl Drop for ReentrancyGuard {
    fn drop(&mut self) {
        self.env.storage().temporary().remove(&GuardKey::Entered);
    }
}

/// Runs `$body` inside a reentrancy guard. `?` and `return` inside the body
/// leave the guard cleanly because it is released when the scope ends.
///
/// ```ignore
/// pub fn withdraw(env: Env, to: Address, amount: i128) -> Result<(), Error> {
///     nonreentrant!(&env, {
///         // checks, effects, then interactions
///         Ok(())
///     })
/// }
/// ```
#[macro_export]
macro_rules! nonreentrant {
    ($env:expr, $body:block) => {{
        let _guard = $crate::guard::ReentrancyGuard::enter($env);
        $body
    }};
}

#[cfg(test)]
mod test {
    use super::*;
    use soroban_sdk::{contract, contractimpl};

    #[contract]
    struct Vault;

    #[contractimpl]
    impl Vault {
        pub fn ping(_env: Env) {}
    }

    fn guarded_call(env: &Env, fail: bool) -> Result<u32, u32> {
        nonreentrant!(env, {
            if fail {
                return Err(7);
            }
            Ok(1)
        })
    }

    #[test]
    fn second_enter_while_active_is_rejected() {
        let env = Env::default();
        let id = env.register(Vault, ());
        env.as_contract(&id, || {
            let _outer = ReentrancyGuard::try_enter(&env).unwrap();
            assert_eq!(
                ReentrancyGuard::try_enter(&env).err(),
                Some(GuardError::Reentrancy)
            );
        });
    }

    #[test]
    fn guard_is_released_on_drop() {
        let env = Env::default();
        let id = env.register(Vault, ());
        env.as_contract(&id, || {
            drop(ReentrancyGuard::try_enter(&env).unwrap());
            assert!(!env.storage().temporary().has(&GuardKey::Entered));
            assert!(ReentrancyGuard::try_enter(&env).is_ok());
        });
    }

    #[test]
    #[should_panic(expected = "#1000")]
    fn recursive_entry_reverts_the_transaction() {
        let env = Env::default();
        let id = env.register(Vault, ());
        env.as_contract(&id, || {
            nonreentrant!(&env, {
                // A cross-contract callback re-entering the contract lands here.
                nonreentrant!(&env, {});
            });
        });
    }

    #[test]
    fn macro_releases_guard_on_early_return() {
        let env = Env::default();
        let id = env.register(Vault, ());
        env.as_contract(&id, || {
            assert_eq!(guarded_call(&env, true), Err(7));
            assert_eq!(guarded_call(&env, false), Ok(1));
        });
    }

    #[test]
    fn guard_is_scoped_to_each_contract() {
        let env = Env::default();
        let first = env.register(Vault, ());
        let second = env.register(Vault, ());
        env.as_contract(&first, || {
            let _guard = ReentrancyGuard::try_enter(&env).unwrap();
            env.as_contract(&second, || {
                assert!(ReentrancyGuard::try_enter(&env).is_ok());
            });
        });
    }
}
