SELECT count(DISTINCT ip.ip) AS "cnt" FROM (
SELECT 
      ip.orig_h AS "ip.ip"
FROM test_zeek.conn_log AS ip
UNION DISTINCT 
SELECT 
      ip.resp_h AS "ip.ip"
FROM test_zeek.conn_log AS ip
) AS __union
