//! Smoke tests for the `deltagraph` binary (Phase 4.1).
//!
//! These exercise the compiled bin via assert_cmd — they catch
//! regressions in the clap surface and the missing-credentials startup
//! error that would otherwise only surface when a user tries the
//! binary on their own machine. They do *not* spin up the full HTTP +
//! Bolt server stack: a wiremock-backed startup integration test
//! belongs in Phase 4.3 (Bolt e2e against Databricks), where it can
//! drive an actual query through the server.
//!
//! Gated on `#[cfg(feature = "databricks")]` because the `deltagraph`
//! binary itself is gated on `required-features = ["databricks"]` in
//! `Cargo.toml` — running these without the feature would fail to
//! locate the binary.

#![cfg(feature = "databricks")]

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn deltagraph_help_lists_databricks_specific_env_vars() {
    // The `--help` summary is the user's entry point — it must mention
    // the DATABRICKS_* env vars or new users have no way to discover
    // them short of reading the source. Pinning the strings here also
    // catches accidental help-text rewrites that drop crucial config.
    Command::cargo_bin("deltagraph")
        .expect("bin")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("DATABRICKS_HOST"))
        .stdout(predicate::str::contains("DATABRICKS_WAREHOUSE_ID"))
        .stdout(predicate::str::contains("DATABRICKS_TOKEN"))
        // The token-on-CLI warning is part of the contract and worth
        // pinning — it's the explicit reason there's no `--token` flag.
        .stdout(predicate::str::contains("env-only"));
}

#[test]
fn deltagraph_startup_without_credentials_errors_clearly() {
    // Spinning the binary with no DATABRICKS_* env vars must exit
    // non-zero with a specific pointer at the missing field. A
    // confusing reqwest connection error here would mean the user
    // can't tell the difference between "wrong creds" and "warehouse
    // unreachable." We scrub the env explicitly so a developer machine
    // with DATABRICKS_HOST already exported doesn't accidentally let
    // the binary attempt a real request.
    Command::cargo_bin("deltagraph")
        .expect("bin")
        .env_remove("DATABRICKS_HOST")
        .env_remove("DATABRICKS_WAREHOUSE_ID")
        .env_remove("DATABRICKS_TOKEN")
        // Pick free-but-uncommon ports so two parallel test runs (or a
        // dev who happens to have something on :7475) don't bind-fail
        // before we even reach the credential check.
        .arg("--http-port")
        .arg("17475")
        .arg("--bolt-port")
        .arg("17687")
        .arg("--disable-bolt")
        .assert()
        .failure()
        .stderr(predicate::str::contains("DATABRICKS_HOST not set"));
}
