SELECT 
      t0.orig_h AS `s.ip`, 
      t0.resp_h AS `d.ip`
FROM test_zeek.conn_log AS t0
