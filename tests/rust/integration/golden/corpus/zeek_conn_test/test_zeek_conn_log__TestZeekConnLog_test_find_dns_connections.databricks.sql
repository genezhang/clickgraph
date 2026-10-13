SELECT 
      v1.orig_h AS `src.ip`, 
      v1.resp_h AS `dst.ip`, 
      v1.proto AS `r.protocol`
FROM test_zeek.conn_log AS v1
WHERE (v1.orig_h IS NOT NULL AND v1.resp_h IS NOT NULL AND v1.service = 'dns')

-- column src.ip: Value [("src.ip", "src.ip")]
-- column dst.ip: Value [("dst.ip", "dst.ip")]
-- column r.protocol: Value [("r.protocol", "r.protocol")]