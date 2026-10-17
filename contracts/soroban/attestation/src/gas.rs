//! Gas profiling cases. Run with
//! `cargo test --manifest-path contracts/soroban/Cargo.toml -p attestation gas_profile_ -- --nocapture --test-threads=1`
//! and feed the output to `tools/gas_profiler`.

use super::test::{setup, DAY};
use super::*;
use soroban_sdk::{testutils::Address as _, Address, BytesN, Env, String, Vec};

const CONTRACT: &str = "attestation";

fn text(env: &Env, len: usize) -> String {
    let buf = [b'a'; 1024];
    String::from_bytes(env, &buf[..len])
}

fn populate(env: &Env, client: &AttestationContractClient<'_>, maintainer: &Address, days: u64) {
    for day in 0..days {
        client.publish_attestation(
            maintainer,
            &day,
            &BytesN::from_array(env, &[day as u8; 32]),
            &String::from_str(env, "seed"),
        );
    }
}

#[test]
fn gas_profile_publish_attestation() {
    for (state, seeded_days) in [("empty", 0u64), ("populated_100", 100u64)] {
        for size in [16usize, 256, 1024] {
            let env = Env::default();
            let (client, maintainer) = setup(&env);
            populate(&env, &client, &maintainer, seeded_days);

            client.publish_attestation(
                &maintainer,
                &DAY,
                &BytesN::from_array(&env, &[7; 32]),
                &text(&env, size),
            );
            gas_profile::record(
                &env,
                CONTRACT,
                "publish_attestation",
                &std::format!("{}/metadata_{}b", state, size),
            );
        }
    }
}

#[test]
fn gas_profile_get_attestation() {
    for (state, seeded_days) in [("single", 1u64), ("populated_100", 100u64)] {
        let env = Env::default();
        let (client, maintainer) = setup(&env);
        populate(&env, &client, &maintainer, seeded_days);

        client.get_attestation(&0);
        gas_profile::record(&env, CONTRACT, "get_attestation", state);
    }
}

#[test]
fn gas_profile_verify_inclusion() {
    for depth in [1u8, 8, 16, 32] {
        let env = Env::default();
        let (client, maintainer) = setup(&env);
        populate(&env, &client, &maintainer, 1);

        let mut proof = Vec::new(&env);
        for i in 0..depth {
            proof.push_back(BytesN::from_array(&env, &[i; 32]));
        }

        client.verify_inclusion(&0, &BytesN::from_array(&env, &[0xff; 32]), &proof);
        gas_profile::record(
            &env,
            CONTRACT,
            "verify_inclusion",
            &std::format!("proof_depth_{}", depth),
        );
    }
}

#[test]
fn gas_profile_maintainer_management() {
    let env = Env::default();
    let (client, maintainer) = setup(&env);

    client.is_maintainer(&maintainer);
    gas_profile::record(&env, CONTRACT, "is_maintainer", "registered");

    let other = Address::generate(&env);
    client.add_maintainer(&other);
    gas_profile::record(&env, CONTRACT, "add_maintainer", "new");

    client.remove_maintainer(&other);
    gas_profile::record(&env, CONTRACT, "remove_maintainer", "registered");
}
