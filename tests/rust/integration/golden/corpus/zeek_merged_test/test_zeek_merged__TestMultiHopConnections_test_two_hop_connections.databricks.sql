SELECT 
      v1.`id.orig_h` AS `a.ip`, 
      v1.`id.resp_h` AS `b.ip`, 
      v3.`id.resp_h` AS `c.ip`, 
      v1.service AS `svc1`, 
      v3.service AS `svc2`
FROM zeek.conn_log AS v1
JOIN zeek.conn_log AS v3 ON v3.`id.orig_h` = v1.`id.resp_h`
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.`id.resp_h` IS NOT NULL AND v3.`id.resp_h` IS NOT NULL AND v1.uid <> v3.uid)
ORDER BY v1.`id.orig_h` ASC NULLS LAST

-- column a.ip: Value [("a.ip", "a.ip")]
-- column b.ip: Value [("b.ip", "b.ip")]
-- column c.ip: Value [("c.ip", "c.ip")]
-- column svc1: Value [("svc1", "svc1")]
-- column svc2: Value [("svc2", "svc2")]