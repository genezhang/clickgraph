WITH with_w1 AS (
SELECT 
      v1.resp_h AS "v3"
FROM test_zeek.conn_log AS v1
WHERE (v1.orig_h IS NOT NULL AND v1.resp_h IS NOT NULL AND v1.orig_h = '192.168.4.76')
)
SELECT 
      w1.v3 AS "peer_ip"
FROM with_w1 AS w1
CROSS JOIN test_zeek.conn_log AS v5
WHERE (v5.orig_h IS NOT NULL AND v5.resp_h IS NOT NULL AND (v5.resp_h = '192.168.4.76' AND v5.orig_h = w1.v3))
