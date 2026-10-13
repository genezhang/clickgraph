SELECT 
      v1.orig_h AS `src.ip`, 
      v1.service AS `r.service`, 
      v1.duration AS `r.duration`
FROM test_zeek.conn_log AS v1
WHERE (v1.orig_h IS NOT NULL AND v1.resp_h IS NOT NULL AND v1.resp_h = '192.168.4.76')
