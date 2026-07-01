-- Views for the zeek_dns_graph reference schema (schemas/examples/zeek_dns_graph.yaml).
-- Model an ARRAY column (dns_log.answers) as first-class graph nodes, with no
-- array-valued node_id. VIEWs are computed on read (no storage) — the array
-- flattening and column normalization happen here so the graph layer stays simple.
-- Run once against your ClickHouse before loading the schema.

-- 1) Flatten the resolved-IP array into one scalar `resolved_ip` per row.
CREATE OR REPLACE VIEW zeek.dns_resolutions AS
SELECT `id.orig_h`        AS src_ip,
       query              AS domain,
       arrayJoin(answers) AS resolved_ip,   -- 1 row per resolved IP
       ts,
       uid,
       qtype_name         AS qtype,
       rcode_name         AS rcode
FROM zeek.dns_log;

-- 2) Unify every IP-bearing column into one `IP` dimension (uniform `ip`).
--    Add more UNION branches as new IP sources arrive (e.g. an access_log).
CREATE OR REPLACE VIEW zeek.all_ips AS
       SELECT `id.orig_h` AS ip FROM zeek.dns_log
UNION DISTINCT
       SELECT resolved_ip AS ip FROM zeek.dns_resolutions;
-- UNION DISTINCT SELECT src_ip AS ip FROM zeek.access_log
-- UNION DISTINCT SELECT dst_ip AS ip FROM zeek.access_log
