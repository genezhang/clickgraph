SELECT 
      r.orig_h AS "src.ip", 
      r.service AS "r.service", 
      r.duration AS "r.duration"
FROM test_zeek.conn_log AS r
WHERE r.resp_h = '192.168.4.76'
