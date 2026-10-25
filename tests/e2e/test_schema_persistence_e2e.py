"""E2E: the persistent schema registry survives a server restart.

Orchestrates its own clickgraph process (Kafka-Connect style: register ->
restart a worker -> config still live), against whatever ClickHouse
CLICKHOUSE_URL points at. This is the headline acceptance run for
`docs/design/SCHEMA_PERSISTENCE.md`, in five acts:

  1. boot with CLICKGRAPH_SCHEMA_PERSISTENCE=clickhouse, register a schema
     through POST /schemas/load, assert `persisted: true`
  2. SIGKILL the server (the spec's crash transition — no graceful
     shutdown), boot it again WITH THE SAME ENV AND NO re-registration
     — the schema must be there (GET /schemas/{name} = 200) and a real
     Cypher query against it must read seeded data (query-after-restore)
  3. DELETE /schemas/{name} -> 200; DELETE again -> 404 (already gone)
  4. kill and boot again — the deleted name must NOT come back
     (the store row is gone; a 404 is the proof)
  5. boot with CLICKGRAPH_SCHEMA_PERSISTENCE unset and re-register: the
     response must NOT carry a `persisted` key — byte-identical to servers
     without the feature (the backward-compatibility contract) — and the
     off-mode server must write NOTHING: the `system.tables` count for the
     database is unchanged and the registry holds no row for the name
     (the spec's zero-writes-off clause).

Requirements:
  - CLICKHOUSE_URL / CLICKHOUSE_USER / CLICKHOUSE_PASSWORD set (a live CH)
  - CLICKGRAPH_BIN pointing at a clickgraph binary (debug or release)
Everything else is self-contained (both the registry table and the dummy
node table live in CLICKHOUSE_DATABASE — the database the client runs in).

The script owns the server lifecycle: on any failure it still reaps the
last spawned process, and it refuses to run against a port 7475 that
already serves something (an orphaned instance from an aborted run would
silently answer every probe instead of the server under test).

Runners:
  - pytest (collectable: `pytest --noconftest
    tests/e2e/test_schema_persistence_e2e.py`, skips when the env of a live
    run is absent) — is how CI drives it. `--noconftest` because
    tests/e2e/conftest.py imports `neo4j`, which this test does not need;
  - plain `python tests/e2e/test_schema_persistence_e2e.py` keeps exit 77
    as the skip indicator and exit 1 as failure, printed as FAIL.
"""

import ctypes

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HTTP_BASE = "http://127.0.0.1"
HTTP_PORT = 7475


def env_args():
    url = os.environ.get("CLICKHOUSE_URL")
    user = os.environ.get("CLICKHOUSE_USER")
    password = os.environ.get("CLICKHOUSE_PASSWORD")
    if not url or not user:
        print("skipping: CLICKHOUSE_URL/CLICKHOUSE_USER are not set")
        return None
    clickgraph_bin = os.environ.get("CLICKGRAPH_BIN")
    if not clickgraph_bin or not os.path.exists(clickgraph_bin):
        print(
            "skipping: CLICKGRAPH_BIN is not set (path to a clickgraph binary, "
            "e.g. target\\release\\clickgraph.exe)"
        )
        return None
    database = os.environ.get("CLICKHOUSE_DATABASE", "default")
    return url, user, password, database, clickgraph_bin


def base_env(url, user, password, database):
    env = dict(os.environ)
    env["CLICKHOUSE_URL"] = url
    env["CLICKHOUSE_USER"] = user
    env["CLICKHOUSE_PASSWORD"] = password
    env["CLICKHOUSE_DATABASE"] = database
    env["CLICKGRAPH_SCHEMA_PERSISTENCE"] = "clickhouse"
    return env


def ch_http(url, user, password, database, query):
    """Run one statement directly via ClickHouse HTTP (setup/teardown only)."""
    sep = "&" if "?" in url else "/?"
    req = urllib.request.Request(
        f"{url}{sep}user={user}&password={password}&database={database}",
        data=query.encode(),
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read().decode()


def spawn(env, clickgraph_bin, log_name="e2e_server.log", log_dir="."):
    # CREATE_NEW_PROCESS_GROUP (Windows): without it, the shutdown signal we
    # send reaches THIS script's own console group and kills the test driver
    # along with the server (witnessed: exit 0xC000013A mid-run).
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    log = open(os.path.join(log_dir, log_name), "a", encoding="utf-8", errors="replace")
    log.write(f"\n===== spawn {time.strftime('%H:%M:%S')} =====\n")
    log.flush()
    return (subprocess.Popen(
        [clickgraph_bin, "--http-port", str(HTTP_PORT), "--disable-bolt"],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        creationflags=flags,
    ), log)


def close_log(handle):
    try:
        handle.close()
    except OSError:
        pass


def wait_healthy(proc, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise SystemExit(f"clickgraph exited early with code {proc.returncode}")
        try:
            with urllib.request.urlopen(
                f"{HTTP_BASE}:{HTTP_PORT}/health", timeout=2
            ) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.3)
    raise SystemExit("clickgraph never became healthy")


def port_must_be_free():
    """Fail fast on an orphaned listener: connecting to /health now would hit
    a DIFFERENT server than the one this test spawns, silently desyncing the
    whole restart-cycle."""
    if port_in_use():
        raise SystemExit(
            f"port {HTTP_PORT} is already in use — stop the process "
            "listening there first"
        )


def request(method, path, body=None):
    """Returns (status, parsed-body) — HTTP error statuses are RETURNED (they
    are part of the API contract this test asserts), only transport failures
    raise."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"{HTTP_BASE}:{HTTP_PORT}{path}",
        data=data,
        method=method,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def kill(proc):
    """Graceful first, forceful as the guarantee: a server that does not act
    on the console event (or whose console event never arrives — the agent
    harness runs this script console-less) MUST still release port 7475, or
    every later act's probes are answered by the WRONG server (witnessed:
    act 5's off-mode assertion failed against the still-alive act-3 server,
    which reported `persisted: true`)."""
    if os.name == "nt":
        # The child has its own process group (see spawn): CTRL_BREAK is the
        # graceful path — tokio's shutdown handler turns it into a clean
        # stop. If it does not land in 5s, the forceful path follows.
        try:
            proc.send_signal(signal.CTRL_BREAK_EVENT)
        except (OSError, ValueError):
            pass
        try:
            proc.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            pass
        _terminate_windows(proc)
    else:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=15)
            return
        except subprocess.TimeoutExpired:
            pass
        proc.kill()  # POSIX: SIGKILL
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass


def _terminate_windows(proc):
    """Unconditional termination, verified. TerminateProcess is called on the
    child's own handle IN this process (a handle value is only meaningful in
    its own table — delegating it to a helper python fails silently with
    ERROR_INVALID_HANDLE, and a silently-surviving server desyncs every later
    act). Recourse is `taskkill /F /T`; the final wait verifies, raising on a
    process that neither ritual can stop."""
    ok = False
    try:
        ok = bool(ctypes.windll.kernel32.TerminateProcess(int(proc._handle), 137))
    except (AttributeError, OSError):
        pass
    if not ok:
        # Access denied on a real process, or the process is already gone
        # (an invalid handle) — taskkill decides; its error output is
        # deliberately discarded (a dead pid is not a failure here).
        subprocess.run(
            ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
            capture_output=True,
        )
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        raise SystemExit(f"clickgraph pid {proc.pid} would not terminate")


def kill_now(proc):
    """The SIGKILL transition the spec pins: no graceful shutdown path, no
    finalization — the process is terminated where it stands (POSIX SIGKILL;
    Windows TerminateProcess via `_terminate_windows`). Whatever the server
    had in flight is lost; only the STORE may survive it."""
    if os.name == "nt":
        _terminate_windows(proc)
    else:
        proc.send_signal(signal.SIGKILL)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass


def port_in_use():
    """True when something accepts connections on the test port. A connect
    probe, not `netstat`: it works on every OS the suite runs on (netstat's
    output differs between Windows and Linux, and is absent from minimal
    images), and a bare connect does not disturb the listener."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        try:
            return s.connect_ex(("127.0.0.1", HTTP_PORT)) == 0
        except OSError:
            return False


def wait_port_free(timeout=15):
    """Between kill and respawn: nothing may still listen. Task-locality
    of the replies depends on it — if an old listener survives its kill, the
    new spawn dies on bind and /health is answered by the old server (silent
    cross-act desync)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not port_in_use():
            return
        time.sleep(0.5)
    raise SystemExit(f"port {HTTP_PORT} is still in use after the kill")


def assert_eq(actual, expected, what):
    if actual != expected:
        raise AssertionError(f"{what}: expected {expected}, got {actual}")


def main():
    log_dir = os.environ.get("E2E_LOG_DIR", ".")
    os.makedirs(log_dir, exist_ok=True)
    args = env_args()
    if not args:
        return 77  # skip indicator for the runner
    url, user, password, database, clickgraph_bin = args
    port_must_be_free()

    name = f"e2e_registry_{os.getpid()}"
    # The node's table must exist for the load path's live discovery read;
    # the node points at the client's own database (explicit `database:`
    # because the YAML grammar requires it).
    ch_http(
        url,
        user,
        password,
        database,
        "CREATE TABLE IF NOT EXISTS e2e_dummy_table "
        "(id UInt32, title String) ENGINE = Memory",
    )
    yaml = (
        f"name: {name}\n"
        "graph_schema:\n"
        "  nodes:\n"
        "    - label: Doc\n"
        f"      database: {database}\n"
        "      table: e2e_dummy_table\n"
        "      node_id: id\n"
        "      property_mappings:\n"
        "        id: id\n"
        "  edges: []\n"
    )
    load_env = base_env(url, user, password, database)

    # The spawned server must not outlive a failed act: a SystemExit on an
    # act boundary otherwise leaves an orphaned listener on 7475 that
    # poisons every later conversation this script has (witnessed twice:
    # probes answered by the PREVIOUS run's server). Each act hands its
    # process to `serve`; `finally` reaps whatever is alive at the end.
    current = None

    def serve(act, env, hard=False):
        nonlocal current
        if current is not None:
            if hard:
                kill_now(current[0])
            else:
                kill(current[0])
            close_log(current[1])
            wait_port_free()
        current = spawn(env, clickgraph_bin, f"e2e_server_{act}.log", log_dir)
        wait_healthy(current[0])

    try:
        # ---- act 1: register -> persisted:true
        serve("act1_register", load_env)
        status, body = request(
            "POST", "/schemas/load", {"schema_name": name, "config_content": yaml}
        )
        assert_eq(status, 200, "POST /schemas/load")
        assert_eq(body.get("persisted"), True, "POST /schemas/load reports persistence")
        assert_eq(
            request("GET", f"/schemas/{name}")[0], 200, f"{name} registered after load"
        )

        # ---- act 2: SIGKILL -> restore, nothing re-registered
        serve("act2_restore", load_env, hard=True)
        status, restored = request("GET", f"/schemas/{name}")
        assert_eq(status, 200, f"{name} restored on boot (restart lost the schema)")
        assert_eq(restored.get("node_types"), 1, "restored schema contents")

        # The restored schema is genuinely queryable, not just listed: seed
        # a row through ClickHouse directly, then run a real Cypher query
        # against the restored graph (query-after-restore). The title is
        # per-run unique, so a crashed earlier run's leftover rows can
        # never make this pass spuriously.
        probe_title = f"e2e probe {name}"
        probe_row = json.dumps({"id": 42, "title": probe_title})
        ch_http(
            url,
            user,
            password,
            database,
            f"INSERT INTO e2e_dummy_table FORMAT JSONEachRow {probe_row}",
        )
        status, qbody = request(
            "POST",
            "/query",
            {"query": "MATCH (d:Doc) WHERE d.id = 42 RETURN d.id, d.title", "schema_name": name},
        )
        assert_eq(status, 200, "query against the restored schema")
        if probe_title not in json.dumps(qbody):
            raise AssertionError(
                "query-after-restore did not read the seeded row through the "
                f"restored schema: {json.dumps(qbody)[:400]}"
            )

        # ---- act 3: DELETE completes the lifecycle; idempotence is a 404
        serve("act3_delete", load_env)
        assert_eq(
            request("GET", f"/schemas/{name}")[0], 200, "schema is live at act 3"
        )
        status, body = request("DELETE", f"/schemas/{name}")
        assert_eq(status, 200, "DELETE /schemas/{name}")
        assert_eq(body.get("persisted"), True, "DELETE clears the stored row")
        assert_eq(
            request("GET", f"/schemas/{name}")[0], 404, "schema gone from memory"
        )
        assert_eq(
            request("DELETE", f"/schemas/{name}")[0], 404, "second DELETE (gone)"
        )

        # ---- act 4: restart -> deleted stays deleted
        serve("act4_stays_deleted", load_env)
        status, _ = request("GET", f"/schemas/{name}")
        if status == 200:
            raise AssertionError(f"{name} resurrected after DELETE (row survived)")
        assert_eq(status, 404, "deleted schema stays deleted after restart")
    finally:
        if current is not None:
            kill(current[0])
            close_log(current[1])

    # ---- act 5: off mode — the backward-compatibility contract
    off_env = dict(load_env)
    off_env.pop("CLICKGRAPH_SCHEMA_PERSISTENCE", None)
    tables_before = int(
        ch_http(
            url,
            user,
            password,
            database,
            f"SELECT count() FROM system.tables WHERE database = '{database}'",
        ).strip()
        or "-1"
    )
    try:
        serve("act5_off_mode", off_env)
        status, body = request(
            "POST", "/schemas/load", {"schema_name": name, "config_content": yaml}
        )
        assert_eq(status, 200, "off-mode POST /schemas/load")
        if "persisted" in body:
            raise AssertionError(
                "off mode must not add a `persisted` field (back-compat "
                f"break): {json.dumps(body)}"
            )
        # The registration is still live in memory — that is pre-feature
        # behavior exactly.
        assert_eq(
            request("GET", f"/schemas/{name}")[0], 200, "off-mode registration live"
        )

        # Zero-writes-off (spec Testing §3.5): with the flag unset the server
        # must not create OR write anything for this feature — the database's
        # table count is unchanged, and the (pre-existing) registry table
        # holds no new row under this name.
        tables_after = int(
            ch_http(
                url,
                user,
                password,
                database,
                f"SELECT count() FROM system.tables WHERE database = '{database}'",
            ).strip()
            or "-2"
        )
        assert_eq(
            tables_after,
            tables_before,
            "off mode must not create any table (system.tables count drift)",
        )
        registry_rows = int(
            ch_http(
                url,
                user,
                password,
                database,
                f"SELECT count() FROM clickgraph_schema_registry "
                f"WHERE name = '{name}'",
            ).strip()
            or "-3"
        )
        assert_eq(registry_rows, 0, "off mode must not write a registry row")
    finally:
        if current is not None:
            kill(current[0])
            close_log(current[1])

    ch_http(url, user, password, database, "DROP TABLE IF EXISTS e2e_dummy_table")
    print(f"OK: persistence round-trip + restart-restore + delete lifecycle for {name}")
    return 0


def test_schema_persistence_restart_cycle():
    """pytest entry point (CI drives this): full restart-cycle acceptance.

    Skips when the environment of a live run is absent (no ClickHouse URL or
    no prebuilt clickgraph binary); every act's assertion fails the test.
    """
    try:
        import pytest
    except ImportError:
        pytest = None

    # main() returns 77 for the env-absent skip; infra failures raise
    # SystemExit, which propagates (pytest reports a failure, and the
    # plain-python __main__ runner maps it to its own error path).
    rc = main()
    if rc == 77:
        if pytest is not None:
            pytest.skip("CLICKHOUSE_URL / CLICKGRAPH_BIN not set")
        return  # run without pytest: nothing to assert about skipping
    assert rc == 0, f"e2e restart cycle failed with exit {rc}"


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (AssertionError, SystemExit) as failure:
        if isinstance(failure, SystemExit) and failure.code == 0:
            raise
        print(f"FAIL: {failure}")
        sys.exit(1)
