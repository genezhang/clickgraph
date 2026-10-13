SELECT 
      v1.`id.orig_h` AS `src.ip`, 
      v1.query AS `d.name`, 
      v1.qtype_name AS `r.qtype`
FROM zeek.dns_log AS v1
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL AND v1.`id.orig_h` = '192.168.1.10')
ORDER BY v1.ts ASC NULLS LAST

-- column src.ip: Value [("src.ip", "src.ip")]
-- column d.name: Value [("d.name", "d.name")]
-- column r.qtype: Value [("r.qtype", "r.qtype")]