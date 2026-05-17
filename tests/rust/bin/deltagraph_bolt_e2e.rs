//! End-to-end Bolt boot test for the `deltagraph` binary (Phase 4.3).
//!
//! Spawns the actual `deltagraph` server subprocess against a wiremock
//! Databricks endpoint, waits for the Bolt TCP port to bind, then
//! performs the Bolt v5 handshake (magic preamble + version proposal,
//! expect server to pick a version). This proves three things end-to-end:
//!
//!   1. The Databricks dispatch in `server::run_with_config` actually
//!      builds an executor from `DATABRICKS_*` env vars (the
//!      `DATABRICKS_BASE_URL` redirection knob keeps us off the real
//!      workspace).
//!   2. The HTTP + Bolt servers boot to a listening state — i.e., none
//!      of the ClickHouse-specific server init code path got tripped by
//!      mistake when `databricks=true`.
//!   3. Bolt protocol works unchanged against a Databricks-backed
//!      AppState — the handshake step proves the listener is wired,
//!      not just bound.
//!
//! What this test does NOT do: drive a Cypher query end-to-end through
//! Bolt → Databricks. That requires a full PackStream client + RUN/PULL
//! sequencing, which is the natural follow-up once we add neo4rs (or
//! similar) as a dev-dep. The current scope is deliberately the
//! cheapest credible regression net for "Bolt + Databricks boot."
//!
//! Gated on `feature = "databricks"` because `deltagraph` itself is.

#![cfg(feature = "databricks")]

use std::io::Write;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::time::{Duration, Instant};

use serde_json::json;
use tempfile::NamedTempFile;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::time::sleep;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Ask the OS for a free TCP port on localhost. We bind a
/// `std::net::TcpListener` to `127.0.0.1:0`, read the assigned port,
/// then drop the listener. There's a small race where another process
/// could grab the port between drop and the subprocess binding it —
/// in practice this is rare on a CI box and beats hard-coding ports
/// that would collide between parallel test runs or with a dev server
/// already on the default 7475/7687.
fn pick_free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

const TEST_SCHEMA_YAML: &str = r#"
name: deltagraph_bolt_e2e
graph_schema:
  nodes:
    - label: User
      database: test_db
      table: users
      node_id: user_id
      property_mappings:
        user_id: user_id
        name: full_name
  edges:
    - type: FOLLOWS
      database: test_db
      table: follows
      from_node: User
      to_node: User
      from_id: follower_id
      to_id: followed_id
      property_mappings: {}
"#;

fn write_schema() -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(TEST_SCHEMA_YAML.as_bytes()).expect("write");
    f.flush().expect("flush");
    f
}

/// Poll the Bolt port until something accepts a TCP connection or the
/// deadline expires. A bare `tokio::time::sleep` would race against
/// slow CI machines; an unbounded loop would mask hung subprocesses.
async fn wait_for_bolt_listen(addr: SocketAddr, deadline: Duration) -> std::io::Result<TcpStream> {
    let start = Instant::now();
    loop {
        match TcpStream::connect(addr).await {
            Ok(s) => return Ok(s),
            Err(_) if start.elapsed() < deadline => sleep(Duration::from_millis(100)).await,
            Err(e) => return Err(e),
        }
    }
}

/// Run the canonical Bolt v5 handshake against an open TCP stream:
/// 4-byte magic preamble (0x60 0x60 0xB0 0x17), then 4 version slots
/// (each 4 bytes, big-endian; trailing zeros for unused slots).
/// The server replies with 4 bytes — either the chosen version or
/// zeros if no proposed version is supported.
async fn bolt_handshake(stream: &mut TcpStream) -> std::io::Result<[u8; 4]> {
    // Magic + propose four current Bolt 5.x minors. Server's
    // SUPPORTED_VERSIONS today is 5.8 → 5.0; offering the latest
    // three keeps the test forward-compatible if 5.0/5.1 are dropped
    // later, and 4.4 stays as a wide-net fallback. Negotiation picks
    // the highest version supported by both sides.
    let mut preamble = vec![0x60u8, 0x60, 0xB0, 0x17];
    preamble.extend_from_slice(&0x0000_0508u32.to_be_bytes()); // 5.8
    preamble.extend_from_slice(&0x0000_0507u32.to_be_bytes()); // 5.7
    preamble.extend_from_slice(&0x0000_0506u32.to_be_bytes()); // 5.6
    preamble.extend_from_slice(&0x0000_0404u32.to_be_bytes()); // 4.4
    stream.write_all(&preamble).await?;

    let mut chosen = [0u8; 4];
    stream.read_exact(&mut chosen).await?;
    Ok(chosen)
}

/// `tokio::process::Child` with `kill_on_drop(true)` sends SIGKILL on
/// drop but does not reap the process — the kernel keeps a zombie
/// entry until `wait()` is called. For panic-path cleanup (where the
/// test's explicit `child.wait().await` is skipped) we hand the child
/// off to a detached `wait()` task so the OS reclaims the entry and
/// releases the bound ports immediately rather than at process exit.
/// `try_current()` is required because the Drop may run after the
/// `#[tokio::test]` runtime has shut down.
struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.start_kill();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deltagraph_boots_and_completes_bolt_handshake() {
    // wiremock for the Databricks SQL endpoint — the server initializes
    // the executor on startup but doesn't immediately call it; this
    // exists in case any startup probe ever does.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-test",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [] }},
            "result": { "data_array": [] }
        })))
        .mount(&mock)
        .await;

    let schema = write_schema();
    let mock_uri = mock.uri();

    // Dynamic ports avoid colliding with a dev `deltagraph`/`clickgraph`
    // on the default 7475/7687 and let parallel test runs coexist.
    let http_port = pick_free_port();
    let bolt_port = pick_free_port();
    let bolt_addr: SocketAddr = format!("127.0.0.1:{bolt_port}").parse().unwrap();

    let child = Command::new(env!("CARGO_BIN_EXE_deltagraph"))
        .env("GRAPH_CONFIG_PATH", schema.path())
        .env("DATABRICKS_HOST", "ignored.cloud.databricks.com")
        .env("DATABRICKS_WAREHOUSE_ID", "wh-bolt-e2e")
        .env("DATABRICKS_TOKEN", "dapi-bolt-e2e")
        .env("DATABRICKS_BASE_URL", &mock_uri)
        // Quiet the binary's own logs so test output stays useful.
        // server boot messages go through env_logger.
        .env("RUST_LOG", "error")
        .arg("--http-port")
        .arg(http_port.to_string())
        .arg("--bolt-port")
        .arg(bolt_port.to_string())
        .arg("--disable-neo4j-compat")
        .kill_on_drop(true)
        .spawn()
        .expect("spawn deltagraph");
    let mut guard = ChildGuard(Some(child));

    // 10 seconds is generous; on a developer laptop the server is
    // listening in under 1s. CI machines under load can take longer.
    let mut bolt = wait_for_bolt_listen(bolt_addr, Duration::from_secs(10))
        .await
        .expect("deltagraph did not bind Bolt port within 10s — likely a startup crash");

    let chosen = bolt_handshake(&mut bolt)
        .await
        .expect("Bolt handshake failed — server is up but did not respond to preamble");

    // A non-zero 4-byte response means the server picked a Bolt
    // version. We don't pin the exact bytes because the server's
    // supported version set may shift; what we care about is that
    // *some* version was accepted (the Bolt protocol layer didn't
    // get tripped by the Databricks-backed AppState).
    assert!(
        chosen != [0, 0, 0, 0],
        "server returned all-zero Bolt version (= 'no version supported'). \
         Bolt protocol negotiation is broken in deltagraph; got bytes {:?}",
        chosen
    );

    // Clean shutdown so the ports are free for subsequent runs.
    if let Some(mut child) = guard.0.take() {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}
