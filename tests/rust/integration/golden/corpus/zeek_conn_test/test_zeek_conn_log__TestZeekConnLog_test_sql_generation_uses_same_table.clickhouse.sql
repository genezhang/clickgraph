SELECT 
      v1.orig_h AS "s.ip", 
      v1.resp_h AS "d.ip"
FROM test_zeek.conn_log AS v1
WHERE (v1.orig_h IS NOT NULL AND v1.resp_h IS NOT NULL)
