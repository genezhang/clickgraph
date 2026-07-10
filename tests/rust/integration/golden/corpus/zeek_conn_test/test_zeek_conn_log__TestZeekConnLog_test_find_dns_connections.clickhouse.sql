SELECT 
      r.orig_h AS "src.ip", 
      r.resp_h AS "dst.ip", 
      r.proto AS "r.protocol"
FROM test_zeek.conn_log AS r
WHERE r.service = 'dns'
