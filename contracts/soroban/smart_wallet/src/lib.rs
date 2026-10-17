#![no_std]

use soroban_sdk::{
    contract, contracterror, contractimpl, contracttype, symbol_short, token, Address, Env, Map,
    Symbol, Val, Vec,
};

const DAY_SECONDS: u64 = 86_400;

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Owner,
    Guardians,
    RecoveryThreshold,
    PerTxCap,
    DailyCap,
    DayStart,
    SpentToday,
    AdminDelay,
    Nonce,
    Sessions,
    PendingCaps,
    Recovery,
    NextRecoveryId,
}

/// Cap change that only becomes applicable once `eta` has passed.
#[derive(Clone)]
#[contracttype]
pub struct PendingCaps {
    pub per_tx_cap: i128,
    pub daily_cap: i128,
    pub eta: u64,
}

#[derive(Clone)]
#[contracttype]
pub struct RecoveryProposal {
    pub id: u32,
    pub new_owner: Address,
    pub approvals: Vec<Address>,
}

#[derive(Clone)]
#[contracttype]
pub struct WalletState {
    pub owner: Address,
    pub guardians: Vec<Address>,
    pub recovery_threshold: u32,
    pub per_tx_cap: i128,
    pub daily_cap: i128,
    pub spent_today: i128,
    pub day_start: u64,
    pub admin_delay: u64,
    pub nonce: u64,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum WalletError {
    NotInitialized = 1,
    AlreadyInitialized = 2,
    InvalidThreshold = 3,
    NotGuardian = 4,
    PerTxCapExceeded = 5,
    DailyCapExceeded = 6,
    InvalidNonce = 7,
    SessionNotFound = 8,
    SessionExpired = 9,
    NoPendingCaps = 10,
    DelayNotElapsed = 11,
    NoPendingRecovery = 12,
    ProposalMismatch = 13,
    AlreadyApproved = 14,
    InvalidAmount = 15,
}

#[contract]
pub struct SmartWallet;

#[contractimpl]
impl SmartWallet {
    /// Runs at deployment so the wallet can never exist uninitialized.
    pub fn __constructor(
        env: Env,
        owner: Address,
        guardians: Vec<Address>,
        recovery_threshold: u32,
        per_tx_cap: i128,
        daily_cap: i128,
        admin_delay: u64,
    ) -> Result<(), WalletError> {
        if env.storage().instance().has(&DataKey::Owner) {
            return Err(WalletError::AlreadyInitialized);
        }
        if recovery_threshold == 0 || recovery_threshold > guardians.len() {
            return Err(WalletError::InvalidThreshold);
        }
        if per_tx_cap <= 0 || daily_cap < per_tx_cap {
            return Err(WalletError::InvalidAmount);
        }

        let s = env.storage().instance();
        s.set(&DataKey::Owner, &owner);
        s.set(&DataKey::Guardians, &guardians);
        s.set(&DataKey::RecoveryThreshold, &recovery_threshold);
        s.set(&DataKey::PerTxCap, &per_tx_cap);
        s.set(&DataKey::DailyCap, &daily_cap);
        s.set(&DataKey::DayStart, &env.ledger().timestamp());
        s.set(&DataKey::SpentToday, &0i128);
        s.set(&DataKey::AdminDelay, &admin_delay);
        s.set(&DataKey::Nonce, &0u64);
        s.set(&DataKey::Sessions, &Map::<Address, u64>::new(&env));
        s.set(&DataKey::NextRecoveryId, &0u32);
        Ok(())
    }

    /// Owner-only arbitrary contract invocation. `value` is the amount the
    /// call is accounted for against the per-transaction and daily caps.
    /// `nonce` must equal the wallet's current nonce, which makes a signed
    /// invocation single-use.
    pub fn execute(
        env: Env,
        target: Address,
        func: Symbol,
        args: Vec<Val>,
        value: i128,
        nonce: u64,
    ) -> Result<Val, WalletError> {
        let owner = Self::owner(&env)?;
        owner.require_auth();
        Self::consume_nonce(&env, nonce)?;
        Self::charge(&env, value)?;

        let result = env.invoke_contract::<Val>(&target, &func, args);
        env.events()
            .publish((symbol_short!("executed"), target), value);
        Ok(result)
    }

    /// Token transfer signed by a session key. Session keys can do nothing
    /// else, so the amount charged against the caps is the amount actually
    /// moved.
    pub fn session_transfer(
        env: Env,
        session_key: Address,
        asset: Address,
        to: Address,
        amount: i128,
        nonce: u64,
    ) -> Result<(), WalletError> {
        session_key.require_auth();

        let sessions: Map<Address, u64> = env.storage().instance().get(&DataKey::Sessions).unwrap();
        let expires_at = sessions
            .get(session_key.clone())
            .ok_or(WalletError::SessionNotFound)?;
        if env.ledger().timestamp() >= expires_at {
            return Err(WalletError::SessionExpired);
        }

        Self::consume_nonce(&env, nonce)?;
        Self::charge(&env, amount)?;

        token::Client::new(&env, &asset).transfer(&env.current_contract_address(), &to, &amount);
        env.events().publish((symbol_short!("sess_pay"), session_key), amount);
        Ok(())
    }

    pub fn add_session_key(env: Env, key: Address, expires_at: u64) -> Result<(), WalletError> {
        Self::owner(&env)?.require_auth();
        let mut sessions: Map<Address, u64> = env.storage().instance().get(&DataKey::Sessions).unwrap();
        sessions.set(key, expires_at);
        env.storage().instance().set(&DataKey::Sessions, &sessions);
        Ok(())
    }

    pub fn revoke_session_key(env: Env, key: Address) -> Result<(), WalletError> {
        Self::owner(&env)?.require_auth();
        let mut sessions: Map<Address, u64> = env.storage().instance().get(&DataKey::Sessions).unwrap();
        sessions.remove(key);
        env.storage().instance().set(&DataKey::Sessions, &sessions);
        Ok(())
    }

    /// Queues a cap change; it can only be applied after the admin delay.
    pub fn propose_caps(env: Env, per_tx_cap: i128, daily_cap: i128) -> Result<(), WalletError> {
        Self::owner(&env)?.require_auth();
        if per_tx_cap <= 0 || daily_cap < per_tx_cap {
            return Err(WalletError::InvalidAmount);
        }
        let delay: u64 = env.storage().instance().get(&DataKey::AdminDelay).unwrap();
        let pending = PendingCaps {
            per_tx_cap,
            daily_cap,
            eta: env.ledger().timestamp() + delay,
        };
        env.storage().instance().set(&DataKey::PendingCaps, &pending);
        Ok(())
    }

    pub fn apply_caps(env: Env) -> Result<(), WalletError> {
        Self::owner(&env)?.require_auth();
        let pending: PendingCaps = env
            .storage()
            .instance()
            .get(&DataKey::PendingCaps)
            .ok_or(WalletError::NoPendingCaps)?;
        if env.ledger().timestamp() < pending.eta {
            return Err(WalletError::DelayNotElapsed);
        }
        env.storage().instance().set(&DataKey::PerTxCap, &pending.per_tx_cap);
        env.storage().instance().set(&DataKey::DailyCap, &pending.daily_cap);
        env.storage().instance().remove(&DataKey::PendingCaps);
        Ok(())
    }

    pub fn cancel_caps(env: Env) -> Result<(), WalletError> {
        Self::owner(&env)?.require_auth();
        env.storage().instance().remove(&DataKey::PendingCaps);
        Ok(())
    }

    /// A guardian opens a recovery proposal; their approval counts as the first.
    pub fn propose_recovery(env: Env, guardian: Address, new_owner: Address) -> Result<u32, WalletError> {
        guardian.require_auth();
        Self::require_guardian(&env, &guardian)?;

        let id: u32 = env.storage().instance().get(&DataKey::NextRecoveryId).unwrap();
        let mut approvals = Vec::new(&env);
        approvals.push_back(guardian);
        let proposal = RecoveryProposal { id, new_owner, approvals };

        env.storage().instance().set(&DataKey::NextRecoveryId, &(id + 1));
        env.storage().instance().set(&DataKey::Recovery, &proposal);
        Self::finish_recovery_if_ready(&env, proposal);
        Ok(id)
    }

    /// Each guardian can approve a proposal once. When M approvals are
    /// collected the owner key is rotated, sessions are dropped and the
    /// proposal is deleted so it cannot be replayed.
    pub fn approve_recovery(env: Env, guardian: Address, proposal_id: u32) -> Result<(), WalletError> {
        guardian.require_auth();
        Self::require_guardian(&env, &guardian)?;

        let mut proposal: RecoveryProposal = env
            .storage()
            .instance()
            .get(&DataKey::Recovery)
            .ok_or(WalletError::NoPendingRecovery)?;
        if proposal.id != proposal_id {
            return Err(WalletError::ProposalMismatch);
        }
        if proposal.approvals.contains(&guardian) {
            return Err(WalletError::AlreadyApproved);
        }
        proposal.approvals.push_back(guardian);
        env.storage().instance().set(&DataKey::Recovery, &proposal);
        Self::finish_recovery_if_ready(&env, proposal);
        Ok(())
    }

    pub fn get_state(env: Env) -> Result<WalletState, WalletError> {
        let owner = Self::owner(&env)?;
        let s = env.storage().instance();
        Ok(WalletState {
            owner,
            guardians: s.get(&DataKey::Guardians).unwrap(),
            recovery_threshold: s.get(&DataKey::RecoveryThreshold).unwrap(),
            per_tx_cap: s.get(&DataKey::PerTxCap).unwrap(),
            daily_cap: s.get(&DataKey::DailyCap).unwrap(),
            spent_today: s.get(&DataKey::SpentToday).unwrap(),
            day_start: s.get(&DataKey::DayStart).unwrap(),
            admin_delay: s.get(&DataKey::AdminDelay).unwrap(),
            nonce: s.get(&DataKey::Nonce).unwrap(),
        })
    }

    pub fn get_pending_caps(env: Env) -> Option<PendingCaps> {
        env.storage().instance().get(&DataKey::PendingCaps)
    }

    fn owner(env: &Env) -> Result<Address, WalletError> {
        env.storage()
            .instance()
            .get(&DataKey::Owner)
            .ok_or(WalletError::NotInitialized)
    }

    fn require_guardian(env: &Env, guardian: &Address) -> Result<(), WalletError> {
        let guardians: Vec<Address> = env
            .storage()
            .instance()
            .get(&DataKey::Guardians)
            .ok_or(WalletError::NotInitialized)?;
        if guardians.contains(guardian) {
            Ok(())
        } else {
            Err(WalletError::NotGuardian)
        }
    }

    fn consume_nonce(env: &Env, nonce: u64) -> Result<(), WalletError> {
        let current: u64 = env.storage().instance().get(&DataKey::Nonce).unwrap();
        if nonce != current {
            return Err(WalletError::InvalidNonce);
        }
        env.storage().instance().set(&DataKey::Nonce, &(current + 1));
        Ok(())
    }

    /// Applies the per-transaction cap and the rolling 24h velocity cap.
    fn charge(env: &Env, value: i128) -> Result<(), WalletError> {
        if value < 0 {
            return Err(WalletError::InvalidAmount);
        }
        let s = env.storage().instance();
        let per_tx_cap: i128 = s.get(&DataKey::PerTxCap).unwrap();
        if value > per_tx_cap {
            return Err(WalletError::PerTxCapExceeded);
        }

        let now = env.ledger().timestamp();
        let mut day_start: u64 = s.get(&DataKey::DayStart).unwrap();
        let mut spent: i128 = s.get(&DataKey::SpentToday).unwrap();
        if now >= day_start + DAY_SECONDS {
            day_start = now;
            spent = 0;
        }

        let daily_cap: i128 = s.get(&DataKey::DailyCap).unwrap();
        let new_spent = spent.checked_add(value).ok_or(WalletError::DailyCapExceeded)?;
        if new_spent > daily_cap {
            return Err(WalletError::DailyCapExceeded);
        }

        s.set(&DataKey::DayStart, &day_start);
        s.set(&DataKey::SpentToday, &new_spent);
        Ok(())
    }

    fn finish_recovery_if_ready(env: &Env, proposal: RecoveryProposal) {
        let threshold: u32 = env.storage().instance().get(&DataKey::RecoveryThreshold).unwrap();
        if proposal.approvals.len() >= threshold {
            let s = env.storage().instance();
            s.set(&DataKey::Owner, &proposal.new_owner);
            s.set(&DataKey::Sessions, &Map::<Address, u64>::new(env));
            s.remove(&DataKey::PendingCaps);
            s.remove(&DataKey::Recovery);
            env.events().publish((symbol_short!("recovered"),), proposal.new_owner);
        }
    }
}

#[cfg(test)]
mod test;
