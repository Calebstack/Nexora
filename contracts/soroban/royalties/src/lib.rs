#![no_std]

use soroban_sdk::{
    contract, contracterror, contractimpl, contracttype, symbol_short, token, Address, Env, Vec,
};

const BPS_DENOMINATOR: i128 = 10_000;

#[derive(Clone)]
#[contracttype]
pub enum DataKey {
    Config,
}

#[derive(Clone)]
#[contracttype]
pub struct RoyaltyConfig {
    pub asset: Address,
    pub creator: Address,
    pub signers: Vec<Address>,
    pub threshold: u32,
    pub royalty_bps: u32,
    pub max_royalty_bps: u32,
}

#[contracterror]
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(u32)]
pub enum RoyaltyError {
    NotInitialized = 1,
    InvalidBasisPoints = 2,
    InvalidThreshold = 3,
    InvalidSalePrice = 4,
    RoyaltyExceedsMaximum = 5,
    NotSigner = 6,
    DuplicateApprover = 7,
    InsufficientApprovals = 8,
    Overflow = 9,
}

#[contract]
pub struct Royalties;

#[contractimpl]
impl Royalties {
    /// Runs at deployment so the royalty terms can never be claimed by the
    /// first caller of a separate `initialize`. `max_royalty_bps` is the hard
    /// ceiling (for example 2500 for 25%) that no sale may exceed.
    pub fn __constructor(
        env: Env,
        asset: Address,
        creator: Address,
        signers: Vec<Address>,
        threshold: u32,
        royalty_bps: u32,
        max_royalty_bps: u32,
    ) -> Result<(), RoyaltyError> {
        if max_royalty_bps as i128 > BPS_DENOMINATOR || royalty_bps > max_royalty_bps {
            return Err(RoyaltyError::InvalidBasisPoints);
        }
        if threshold == 0 || threshold > signers.len() {
            return Err(RoyaltyError::InvalidThreshold);
        }

        env.storage().instance().set(
            &DataKey::Config,
            &RoyaltyConfig {
                asset,
                creator,
                signers,
                threshold,
                royalty_bps,
                max_royalty_bps,
            },
        );
        Ok(())
    }

    pub fn get_config(env: Env) -> Result<RoyaltyConfig, RoyaltyError> {
        Self::load(&env)
    }

    /// EIP-2981 style quote: the payout address and the royalty owed on a sale
    /// at `sale_price`, rounded down.
    pub fn royalty_info(env: Env, sale_price: i128) -> Result<(Address, i128), RoyaltyError> {
        let config = Self::load(&env)?;
        let royalty = Self::royalty_for(&config, sale_price)?;
        Ok((config.creator, royalty))
    }

    /// Settles a secondary sale. The buyer pays `sale_price`; the royalty goes
    /// to the creator and the remainder to the seller. Both transfers run in
    /// this one invocation, so a failure in either reverts the whole sale.
    pub fn settle_sale(
        env: Env,
        buyer: Address,
        seller: Address,
        sale_price: i128,
    ) -> Result<i128, RoyaltyError> {
        buyer.require_auth();

        let config = Self::load(&env)?;
        let royalty = Self::royalty_for(&config, sale_price)?;

        let token_client = token::Client::new(&env, &config.asset);
        if royalty > 0 {
            token_client.transfer(&buyer, &config.creator, &royalty);
        }
        token_client.transfer(&buyer, &seller, &(sale_price - royalty));

        env.events()
            .publish((symbol_short!("royalty"), config.creator), (seller, sale_price, royalty));
        Ok(royalty)
    }

    /// Moves royalty payouts to `new_address`. At least `threshold` distinct
    /// registered signers must authorize the change.
    pub fn update_payout_address(
        env: Env,
        new_address: Address,
        approvers: Vec<Address>,
    ) -> Result<(), RoyaltyError> {
        let mut config = Self::load(&env)?;

        for (i, approver) in approvers.iter().enumerate() {
            if !config.signers.contains(&approver) {
                return Err(RoyaltyError::NotSigner);
            }
            for earlier in approvers.iter().take(i) {
                if earlier == approver {
                    return Err(RoyaltyError::DuplicateApprover);
                }
            }
        }
        if approvers.len() < config.threshold {
            return Err(RoyaltyError::InsufficientApprovals);
        }
        for approver in approvers.iter() {
            approver.require_auth();
        }

        config.creator = new_address.clone();
        env.storage().instance().set(&DataKey::Config, &config);
        env.events()
            .publish((symbol_short!("payout"), symbol_short!("updated")), new_address);
        Ok(())
    }

    fn load(env: &Env) -> Result<RoyaltyConfig, RoyaltyError> {
        env.storage()
            .instance()
            .get(&DataKey::Config)
            .ok_or(RoyaltyError::NotInitialized)
    }

    fn royalty_for(config: &RoyaltyConfig, sale_price: i128) -> Result<i128, RoyaltyError> {
        if sale_price <= 0 {
            return Err(RoyaltyError::InvalidSalePrice);
        }
        if config.royalty_bps > config.max_royalty_bps {
            return Err(RoyaltyError::RoyaltyExceedsMaximum);
        }
        sale_price
            .checked_mul(config.royalty_bps as i128)
            .map(|scaled| scaled / BPS_DENOMINATOR)
            .ok_or(RoyaltyError::Overflow)
    }
}

#[cfg(test)]
mod test;
