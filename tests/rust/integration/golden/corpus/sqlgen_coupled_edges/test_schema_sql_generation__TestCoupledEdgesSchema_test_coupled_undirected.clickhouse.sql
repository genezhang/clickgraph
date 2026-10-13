WITH v1_both AS (
SELECT 
      e.conn_state AS "conn_state", 
      e.duration AS "duration", 
      e."id.orig_h" AS "id.orig_h", 
      e."id.resp_h" AS "id.resp_h", 
      e.orig_bytes AS "orig_bytes", 
      e.proto AS "proto", 
      e.resp_bytes AS "resp_bytes", 
      e.service AS "service", 
      e.ts AS "ts", 
      e.uid AS "uid", 
      e."id.orig_h" AS "__cg_start_0", 
      e."id.resp_h" AS "__cg_end_0"
FROM zeek.conn_log AS e

UNION ALL
SELECT 
      e.conn_state AS "conn_state", 
      e.duration AS "duration", 
      e."id.orig_h" AS "id.orig_h", 
      e."id.resp_h" AS "id.resp_h", 
      e.orig_bytes AS "orig_bytes", 
      e.proto AS "proto", 
      e.resp_bytes AS "resp_bytes", 
      e.service AS "service", 
      e.ts AS "ts", 
      e.uid AS "uid", 
      e."id.resp_h" AS "__cg_start_0", 
      e."id.orig_h" AS "__cg_end_0"
FROM zeek.conn_log AS e
WHERE e."id.orig_h" <> e."id.resp_h"
), 
__cg_nodes_IP__cg_rows AS (
SELECT 
      e."id.orig_h" AS "ip"
FROM zeek.dns_log AS e

UNION ALL
SELECT 
      e."id.resp_h" AS "ip"
FROM zeek.dns_log AS e
), 
__cg_nodes_IP AS (
SELECT 
      e.ip AS "ip"
FROM __cg_nodes_IP__cg_rows AS e
WHERE NOT isNull(e.ip)
GROUP BY e.ip
)
SELECT 
      v0.ip AS "ip.ip", 
      v2.ip AS "other.ip"
FROM __cg_nodes_IP AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.ip
JOIN __cg_nodes_IP AS v2 ON v1.__cg_end_0 = v2.ip

-- column ip.ip: Value [("ip.ip", "ip.ip")]
-- column other.ip: Value [("other.ip", "other.ip")]