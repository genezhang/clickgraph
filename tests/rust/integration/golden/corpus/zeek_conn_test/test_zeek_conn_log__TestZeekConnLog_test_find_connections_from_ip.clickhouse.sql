SELECT 
      v1.orig_h AS "src.ip", 
      v1.resp_h AS "dst.ip", 
      v1.service AS "r.service"
FROM test_zeek.conn_log AS v1
WHERE (v1.orig_h IS NOT NULL AND v1.resp_h IS NOT NULL AND v1.orig_h = '192.168.4.76')
ORDER BY v1.ts ASC

-- column src.ip: Value [("src.ip", "src.ip")]
-- column dst.ip: Value [("dst.ip", "dst.ip")]
-- column r.service: Value [("r.service", "r.service")]