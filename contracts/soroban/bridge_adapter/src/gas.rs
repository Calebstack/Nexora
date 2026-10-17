//! Gas profiling cases. Run with
//! `cargo test --manifest-path contracts/soroban/Cargo.toml -p bridge_adapter gas_profile_ -- --nocapture --test-threads=1`
//! and feed the output to `tools/gas_profiler`.

use super::test::{public_keys, setup, signing_keys};
use super::*;
use soroban_sdk::{testutils::Address as _, token::StellarAssetClient, Address, Bytes, Env};

const CONTRACT: &str = "bridge_adapter";

#[test]
fn gas_profile_release() {
    for (validators, threshold, signers) in [(3u8, 2u32, 2usize), (7, 4, 4), (15, 8, 8), (31, 16, 16)] {
        for (mode, mint_mode) in [("lock", false), ("mint", true)] {
            let env = Env::default();
            let bridge = setup(&env, mint_mode, validators, threshold);
            let recipient = Address::generate(&env);
            let signers: std::vec::Vec<usize> = (0..signers).collect();

            let sigs = bridge.release_signatures(&signers, &recipient, 500, 1);
            bridge.client.release(&recipient, &500, &1, &sigs);
            gas_profile::record(
                &env,
                CONTRACT,
                "release",
                &std::format!("{}/{}_of_{}", mode, threshold, validators),
            );
        }
    }
}

#[test]
fn gas_profile_rotate_validators() {
    for (validators, threshold) in [(3u8, 2u32), (7, 4), (15, 8)] {
        let env = Env::default();
        let bridge = setup(&env, false, validators, threshold);
        let new_set = public_keys(&env, &signing_keys(validators + 2));
        let new_threshold = threshold + 1;
        let signers: std::vec::Vec<usize> = (0..threshold as usize).collect();

        let sigs = bridge.rotation_signatures(&signers, &new_set, new_threshold, 1);
        bridge
            .client
            .rotate_validators(&new_set, &new_threshold, &1, &sigs);
        gas_profile::record(
            &env,
            CONTRACT,
            "rotate_validators",
            &std::format!("{}_of_{}", threshold, validators),
        );
    }
}

#[test]
fn gas_profile_deposit() {
    for (mode, mint_mode) in [("lock", false), ("mint", true)] {
        let env = Env::default();
        let bridge = setup(&env, mint_mode, 3, 2);
        let user = Address::generate(&env);
        StellarAssetClient::new(&env, &bridge.token).mint(&user, &1_000);

        bridge
            .client
            .deposit(&user, &500, &Bytes::from_slice(&env, b"dest-address"));
        gas_profile::record(&env, CONTRACT, "deposit", mode);
    }
}

#[test]
fn gas_profile_admin_and_views() {
    let env = Env::default();
    let bridge = setup(&env, false, 15, 8);

    bridge.client.set_paused(&true);
    gas_profile::record(&env, CONTRACT, "set_paused", "pause");

    bridge.client.is_paused();
    gas_profile::record(&env, CONTRACT, "is_paused", "paused");

    bridge.client.get_validators();
    gas_profile::record(&env, CONTRACT, "get_validators", "15_validators");

    bridge.client.get_threshold();
    gas_profile::record(&env, CONTRACT, "get_threshold", "default");

    bridge.client.is_nonce_used(&1);
    gas_profile::record(&env, CONTRACT, "is_nonce_used", "unused");
}
