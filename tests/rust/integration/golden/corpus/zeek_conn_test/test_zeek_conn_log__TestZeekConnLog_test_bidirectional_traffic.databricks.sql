WITH with_peer_ip_cte_1 AS (SELECT 
      t0.resp_h AS `peer_ip`
FROM test_zeek.conn_log AS t0
WHERE t0.orig_h = '192.168.4.76'
)
SELECT 
      peer_ip.peer_ip AS `peer_ip.peer_ip`
FROM test_zeek.conn_log AS t1
INNER JOIN with_peer_ip_cte_1 AS peer_ip ON t1.orig_h = peer_ip.peer_ip
WHERE (t1.orig_h = peer_ip.peer_ip AND t1.resp_h = '192.168.4.76')
