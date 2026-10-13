SELECT 
      v1.`id.orig_h` AS `a.ip`, 
      v1.`id.resp_h` AS `b.ip`, 
      v3.`id.resp_h` AS `c.ip`
FROM zeek.conn_log AS v1
JOIN zeek.conn_log AS v3 ON v3.`id.orig_h` = v1.`id.resp_h`
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.`id.resp_h` IS NOT NULL AND v3.`id.resp_h` IS NOT NULL AND v1.uid <> v3.uid)
