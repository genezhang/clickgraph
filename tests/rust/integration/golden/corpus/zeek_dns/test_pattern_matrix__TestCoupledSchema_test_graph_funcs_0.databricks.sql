SELECT 
      'REQUESTED' AS `type(r)`, 
      v1.`id.orig_h` AS `id(a)`, 
      array('IP') AS `labels(a)`
FROM zeek.dns_log AS v1
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL)
LIMIT 5