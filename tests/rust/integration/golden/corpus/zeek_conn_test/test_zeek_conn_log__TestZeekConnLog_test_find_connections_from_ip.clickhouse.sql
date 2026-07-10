SELECT 
      r.orig_h AS "src.ip", 
      r.resp_h AS "dst.ip", 
      r.service AS "r.service"
FROM test_zeek.conn_log AS r
WHERE r.orig_h = '192.168.4.76'
ORDER BY r.ts ASC
