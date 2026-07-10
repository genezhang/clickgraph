SELECT 
      r.proto AS "r.protocol", 
      r.service AS "r.service", 
      r.duration AS "r.duration", 
      r.orig_bytes AS "r.orig_bytes", 
      r.resp_bytes AS "r.resp_bytes"
FROM test_zeek.conn_log AS r
WHERE r.uid = 'CMdzit1AMNsmfAIiQc'
