# Live-ClickHouse evidence protocol (G6)

Any performance claim ships with measured evidence, or the claim is removed.

1. Start the pinned test database: `docker compose -f
   docker-compose.test.yaml up -d clickhouse-test` (image
   `clickhouse/clickhouse-server:25.8.12`, user `test_user`).
2. Build the fixture with server-side generation (`INSERT ... SELECT FROM
   numbers(N)`); record row count and bytes.
3. `EXPLAIN actions=1` with **default settings** for every query shape:
   the automatic PREWHERE move must be shown, not assumed.
4. Head-to-head timings, best-of-5, `FORMAT Null`, plus row-equality check.
5. Also run with `optimize_move_to_prewhere = 0` to characterize when an
   explicit form could matter.
6. Negative results are results: if no shape wins on defaults, say so and
   do not ship the optimization (precedent: closed #1385).
