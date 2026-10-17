//! Shared helpers for the Soroban invariant suites.
//!
//! Every suite runs through [`check`], which seeds proptest from
//! `INVARIANT_SEED` (a random seed is generated and printed when unset) and,
//! on failure, writes a markdown audit report with the seed, the minimal
//! failing input and the command that reproduces it.

use proptest::prelude::*;
use proptest::test_runner::{Config, RngAlgorithm, TestError, TestRng, TestRunner};
use soroban_sdk::{testutils::Address as _, token, Address, Env};
use std::{fs, path::PathBuf, time::{SystemTime, UNIX_EPOCH}};

const DEFAULT_CASES: u32 = 64;

fn seed() -> u64 {
    std::env::var("INVARIANT_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1)
        })
}

fn cases() -> u32 {
    std::env::var("INVARIANT_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_CASES)
}

fn report_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target").join("invariant-reports")
}

/// Runs `test` against generated inputs. Panics after writing a markdown
/// report if any input violates an invariant.
pub fn check<S, F>(name: &str, strategy: S, test: F)
where
    S: Strategy,
    S::Value: std::fmt::Debug,
    F: Fn(S::Value) -> Result<(), TestCaseError>,
{
    let seed = seed();
    let mut seed_bytes = [0u8; 32];
    for chunk in seed_bytes.chunks_mut(8) {
        chunk.copy_from_slice(&seed.to_le_bytes());
    }
    let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &seed_bytes);
    let config = Config {
        cases: cases(),
        failure_persistence: None,
        ..Config::default()
    };
    let mut runner = TestRunner::new_with_rng(config, rng);

    if let Err(err) = runner.run(&strategy, test) {
        let (reason, input) = match err {
            TestError::Fail(reason, input) => (reason.to_string(), format!("{:#?}", input)),
            TestError::Abort(reason) => (reason.to_string(), "n/a".to_string()),
        };
        let report = format!(
            "# Invariant failure: {name}\n\n\
             - Seed: `{seed}`\n\
             - Reason: {reason}\n\n\
             ## Minimal failing input\n\n```\n{input}\n```\n\n\
             ## Reproduce\n\n```\nINVARIANT_SEED={seed} cargo test --manifest-path contracts/soroban/tests/invariants/Cargo.toml {name}\n```\n"
        );
        let dir = report_dir();
        let _ = fs::create_dir_all(&dir);
        let path = dir.join(format!("{name}.md"));
        let _ = fs::write(&path, &report);
        panic!("invariant `{name}` failed (seed {seed}); report written to {}\n{report}", path.display());
    }
}

/// A Stellar Asset Contract token with a set of tracked holders, so a test
/// can assert that the sum of balances equals the amount minted.
pub struct Ledger<'a> {
    pub env: &'a Env,
    pub asset: Address,
    pub minted: i128,
    holders: std::vec::Vec<Address>,
}

impl<'a> Ledger<'a> {
    pub fn new(env: &'a Env) -> Self {
        let asset = env
            .register_stellar_asset_contract_v2(Address::generate(env))
            .address();
        Ledger { env, asset, minted: 0, holders: std::vec::Vec::new() }
    }

    pub fn track(&mut self, holder: &Address) {
        self.holders.push(holder.clone());
    }

    pub fn mint(&mut self, to: &Address, amount: i128) {
        token::StellarAssetClient::new(self.env, &self.asset).mint(to, &amount);
        self.minted += amount;
    }

    pub fn balance(&self, who: &Address) -> i128 {
        token::Client::new(self.env, &self.asset).balance(who)
    }

    pub fn tracked_total(&self) -> i128 {
        self.holders.iter().map(|h| self.balance(h)).sum()
    }
}
