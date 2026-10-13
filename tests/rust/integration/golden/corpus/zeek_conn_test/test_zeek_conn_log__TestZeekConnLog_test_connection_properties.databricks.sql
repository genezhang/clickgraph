SELECT 
      v1.proto AS `r.protocol`, 
      v1.service AS `r.service`, 
      v1.duration AS `r.duration`, 
      v1.orig_bytes AS `r.orig_bytes`, 
      v1.resp_bytes AS `r.resp_bytes`
FROM test_zeek.conn_log AS v1
WHERE (v1.orig_h IS NOT NULL AND v1.resp_h IS NOT NULL AND v1.uid = 'CMdzit1AMNsmfAIiQc')
