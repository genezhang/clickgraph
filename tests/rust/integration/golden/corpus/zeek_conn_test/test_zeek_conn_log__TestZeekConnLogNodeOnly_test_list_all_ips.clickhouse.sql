SELECT `ip.ip` AS `ip.ip` FROM (
SELECT DISTINCT 
      ip.orig_h AS "ip.ip", 
      ip.orig_h AS "__order_col_0"
FROM test_zeek.conn_log AS ip
UNION DISTINCT 
SELECT DISTINCT 
      ip.resp_h AS "ip.ip", 
      ip.resp_h AS "__order_col_0"
FROM test_zeek.conn_log AS ip
) AS __union
ORDER BY __union.`__order_col_0` ASC
