SELECT 
      r1.`id.orig_h` AS `a.ip`, 
      r2.`id.orig_h` AS `b.ip`, 
      r2.`id.resp_h` AS `c.ip`
FROM zeek.conn_log AS r1
INNER JOIN zeek.conn_log AS r2 ON r2.`id.orig_h` = r1.`id.resp_h`
WHERE (r1.`id.orig_h` = '192.168.1.10' AND r2.uid <> r1.uid)
