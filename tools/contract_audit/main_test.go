package main

import (
	"strings"
	"testing"
)

func scanLines(t *testing.T, src string) []Finding {
	t.Helper()
	return Scan("lib.rs", src)
}

func TestScanFlagsStorageWriteAfterExternalCall(t *testing.T) {
	src := `
pub fn withdraw(env: Env, to: Address, amount: i128) {
    let client = token::Client::new(&env, &asset);
    client.transfer(&env.current_contract_address(), &to, &amount);
    env.storage().persistent().set(&DataKey::Balance, &0);
}
`
	findings := scanLines(t, src)
	if len(findings) != 1 {
		t.Fatalf("got %d findings, want 1: %v", len(findings), findings)
	}
	if findings[0].Line != 5 || !strings.Contains(findings[0].Message, "fn withdraw") || !strings.Contains(findings[0].Message, "line 4") {
		t.Fatalf("unexpected finding: %s", findings[0])
	}
}

func TestScanAcceptsChecksEffectsInteractionsOrder(t *testing.T) {
	src := `
pub fn withdraw(env: Env, to: Address, amount: i128) {
    env.storage().persistent().set(&DataKey::Balance, &0);
    token::Client::new(&env, &asset).transfer(&env.current_contract_address(), &to, &amount);
}
`
	if findings := scanLines(t, src); len(findings) != 0 {
		t.Fatalf("unexpected findings: %v", findings)
	}
}

func TestScanTracksStorageBindings(t *testing.T) {
	src := `
pub fn sign(env: Env) {
    let storage = env.storage().persistent();
    let client = token::Client::new(&env, &asset);
    client.transfer(&a, &b, &1);
    storage.set(&DataKey::Escrow, &escrow);
}
`
	if findings := scanLines(t, src); len(findings) != 1 {
		t.Fatalf("got %d findings, want 1: %v", len(findings), findings)
	}
}

func TestScanFollowsSameFileHelpers(t *testing.T) {
	src := `
fn save(env: &Env) {
    env.storage().instance().set(&DataKey::Config, &1);
}

pub fn settle(env: Env) {
    env.invoke_contract::<()>(&other, &symbol_short!("ping"), args);
    Self::save(&env);
}
`
	findings := scanLines(t, src)
	if len(findings) != 1 || !strings.Contains(findings[0].Message, "call to save") {
		t.Fatalf("unexpected findings: %v", findings)
	}
}

func TestScanIgnoresReadOnlyCallsCommentsAndGuardedFunctions(t *testing.T) {
	src := `
pub fn read_then_write(env: Env) {
    let client = token::Client::new(&env, &asset);
    let held = client.balance(&env.current_contract_address());
    env.storage().persistent().set(&DataKey::Held, &held);
}

pub fn commented(env: Env) {
    // client.transfer(&a, &b, &1);
    let note = "client.transfer(";
    env.storage().persistent().set(&DataKey::Note, &note);
}

pub fn guarded(env: Env) {
    nonreentrant!(&env, {
        token::Client::new(&env, &asset).transfer(&a, &b, &1);
        env.storage().persistent().set(&DataKey::Done, &true);
    });
}
`
	if findings := scanLines(t, src); len(findings) != 0 {
		t.Fatalf("unexpected findings: %v", findings)
	}
}

func TestScanSkipsTestModules(t *testing.T) {
	src := `
#[cfg(test)]
mod tests {
    fn bad(env: Env) {
        token::Client::new(&env, &asset).transfer(&a, &b, &1);
        env.storage().persistent().set(&DataKey::X, &1);
    }
}
`
	if findings := scanLines(t, src); len(findings) != 0 {
		t.Fatalf("unexpected findings: %v", findings)
	}
}
