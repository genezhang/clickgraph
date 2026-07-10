SELECT 
      count(DISTINCT src.orig_h) AS `unique_sources`
FROM test_zeek.conn_log AS t0
