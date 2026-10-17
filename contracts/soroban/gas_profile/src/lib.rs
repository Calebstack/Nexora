//! Test-only helper that turns the resources metered for the last top level
//! contract invocation into a `GAS_PROFILE` line. `tools/gas_profiler` parses
//! those lines into the Markdown and JSON gas reports.

use soroban_sdk::Env;

/// Prints the resources consumed by the most recent top level invocation on
/// `env`. Call it immediately after the invocation being profiled.
pub fn record(env: &Env, contract: &str, function: &str, case: &str) {
    let resources = env.cost_estimate().resources();
    let fee = env.cost_estimate().fee();
    std::println!(
        "GAS_PROFILE {{\"contract\":\"{}\",\"function\":\"{}\",\"case\":\"{}\",\"instructions\":{},\"mem_bytes\":{},\"read_bytes\":{},\"write_bytes\":{},\"fee_stroops\":{}}}",
        contract,
        function,
        case,
        resources.instructions as u64,
        resources.mem_bytes as u64,
        resources.read_bytes as u64,
        resources.write_bytes as u64,
        fee.total as i64,
    );
}
