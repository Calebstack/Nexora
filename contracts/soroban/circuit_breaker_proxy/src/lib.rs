#![no_std]

use soroban_sdk::{contract, contracterror, contractimpl, contracttype, symbol_short, Address, Env, Vec, u64};

#[derive(Clone, Copy)]
#[contracttype]
pub enum CircuitState {
    Closed = 0,
    Open = 1,
    HalfOpen = 2,
}

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Admin,
    Implementation,
    CircuitState,
    FailureCount,
    LastFailureTime,
    FailureThreshold,
    RecoveryTimeout,
    PreviousImplementations,
}

#[derive(Clone)]
#[contracttype]
pub struct CircuitBreakerState {
    pub state: CircuitState,
    pub failure_count: u32,
    pub last_failure_time: u64,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum CircuitError {
    NotInitialized = 1,
    AlreadyInitialized = 2,
    InvalidAdmin = 3,
    CircuitOpen = 4,
    UpgradeFailed = 5,
    InvalidImplementation = 6,
    RecoveryInProgress = 7,
}

#[contract]
pub struct CircuitBreakerProxy;

#[contractimpl]
impl CircuitBreakerProxy {
    pub fn initialize(
        env: Env,
        admin: Address,
        implementation: Address,
        failure_threshold: u32,
        recovery_timeout_secs: u64,
    ) -> Result<(), CircuitError> {
        if env.storage().persistent().has(&DataKey::Admin) {
            return Err(CircuitError::AlreadyInitialized);
        }

        admin.require_auth();

        env.storage().persistent().set(&DataKey::Admin, &admin);
        env.storage().persistent().set(&DataKey::Implementation, &implementation);
        env.storage().persistent().set(
            &DataKey::CircuitState,
            &CircuitBreakerState {
                state: CircuitState::Closed,
                failure_count: 0,
                last_failure_time: 0,
            },
        );
        env.storage()
            .persistent()
            .set(&DataKey::FailureThreshold, &failure_threshold);
        env.storage()
            .persistent()
            .set(&DataKey::RecoveryTimeout, &recovery_timeout_secs);
        env.storage()
            .persistent()
            .set(&DataKey::PreviousImplementations, &Vec::<Address>::new(&env));

        Ok(())
    }

    pub fn upgrade_implementation(
        env: Env,
        new_implementation: Address,
    ) -> Result<(), CircuitError> {
        let admin: Address = env
            .storage()
            .persistent()
            .get(&DataKey::Admin)
            .ok_or(CircuitError::NotInitialized)?;

        admin.require_auth();

        let current_impl: Address = env
            .storage()
            .persistent()
            .get(&DataKey::Implementation)
            .ok_or(CircuitError::NotInitialized)?;

        let mut previous: Vec<Address> = env
            .storage()
            .persistent()
            .get(&DataKey::PreviousImplementations)
            .unwrap_or_else(|| Vec::new(&env));

        previous.push_back(current_impl);
        env.storage()
            .persistent()
            .set(&DataKey::PreviousImplementations, &previous);

        env.storage()
            .persistent()
            .set(&DataKey::Implementation, &new_implementation);

        Ok(())
    }

    pub fn record_failure(env: Env) -> Result<(), CircuitError> {
        let mut state: CircuitBreakerState = env
            .storage()
            .persistent()
            .get(&DataKey::CircuitState)
            .ok_or(CircuitError::NotInitialized)?;

        let threshold: u32 = env
            .storage()
            .persistent()
            .get(&DataKey::FailureThreshold)
            .unwrap_or(5);

        state.failure_count = state.failure_count.saturating_add(1);
        state.last_failure_time = env.ledger().timestamp();

        if state.failure_count >= threshold {
            state.state = CircuitState::Open;
        }

        env.storage()
            .persistent()
            .set(&DataKey::CircuitState, &state);

        Ok(())
    }

    pub fn record_success(env: Env) -> Result<(), CircuitError> {
        let mut state: CircuitBreakerState = env
            .storage()
            .persistent()
            .get(&DataKey::CircuitState)
            .ok_or(CircuitError::NotInitialized)?;

        match state.state {
            CircuitState::Closed => {
                state.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                state.state = CircuitState::Closed;
                state.failure_count = 0;
            }
            CircuitState::Open => {}
        }

        env.storage()
            .persistent()
            .set(&DataKey::CircuitState, &state);

        Ok(())
    }

    pub fn attempt_recovery(env: Env) -> Result<(), CircuitError> {
        let mut state: CircuitBreakerState = env
            .storage()
            .persistent()
            .get(&DataKey::CircuitState)
            .ok_or(CircuitError::NotInitialized)?;

        if state.state != CircuitState::Open {
            return Err(CircuitError::RecoveryInProgress);
        }

        let recovery_timeout: u64 = env
            .storage()
            .persistent()
            .get(&DataKey::RecoveryTimeout)
            .unwrap_or(300);

        let current_time = env.ledger().timestamp();
        if current_time.saturating_sub(state.last_failure_time) < recovery_timeout {
            return Err(CircuitError::CircuitOpen);
        }

        state.state = CircuitState::HalfOpen;
        state.failure_count = 0;

        env.storage()
            .persistent()
            .set(&DataKey::CircuitState, &state);

        Ok(())
    }

    pub fn get_circuit_state(env: Env) -> Result<CircuitBreakerState, CircuitError> {
        env.storage()
            .persistent()
            .get(&DataKey::CircuitState)
            .ok_or(CircuitError::NotInitialized)
    }

    pub fn get_implementation(env: Env) -> Result<Address, CircuitError> {
        env.storage()
            .persistent()
            .get(&DataKey::Implementation)
            .ok_or(CircuitError::NotInitialized)
    }

    pub fn get_previous_implementations(env: Env) -> Result<Vec<Address>, CircuitError> {
        env.storage()
            .persistent()
            .get(&DataKey::PreviousImplementations)
            .ok_or(CircuitError::NotInitialized)
    }

    pub fn is_circuit_open(env: Env) -> Result<bool, CircuitError> {
        let state: CircuitBreakerState = env
            .storage()
            .persistent()
            .get(&DataKey::CircuitState)
            .ok_or(CircuitError::NotInitialized)?;

        Ok(matches!(state.state, CircuitState::Open))
    }

    pub fn reset_circuit(env: Env) -> Result<(), CircuitError> {
        let admin: Address = env
            .storage()
            .persistent()
            .get(&DataKey::Admin)
            .ok_or(CircuitError::NotInitialized)?;

        admin.require_auth();

        let mut state: CircuitBreakerState = env
            .storage()
            .persistent()
            .get(&DataKey::CircuitState)
            .ok_or(CircuitError::NotInitialized)?;

        state.state = CircuitState::Closed;
        state.failure_count = 0;
        state.last_failure_time = 0;

        env.storage()
            .persistent()
            .set(&DataKey::CircuitState, &state);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use soroban_sdk::testutils::{Address as _, Env as _};

    #[test]
    fn test_initialize() {
        let env = Env::default();
        let admin = Address::random(&env);
        let impl_addr = Address::random(&env);

        let result = CircuitBreakerProxy::initialize(env, admin, impl_addr, 5, 300);
        assert!(result.is_ok());
    }

    #[test]
    fn test_circuit_state() {
        let env = Env::default();
        let admin = Address::random(&env);
        let impl_addr = Address::random(&env);

        CircuitBreakerProxy::initialize(env.clone(), admin, impl_addr, 5, 300).unwrap();

        let state = CircuitBreakerProxy::get_circuit_state(env).unwrap();
        assert!(matches!(state.state, CircuitState::Closed));
    }
}
