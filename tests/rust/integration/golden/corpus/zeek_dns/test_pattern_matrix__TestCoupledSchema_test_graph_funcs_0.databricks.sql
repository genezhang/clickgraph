SELECT 
      'REQUESTED::IP::Domain' AS `type(r)`, 
      r.`id.orig_h` AS `id(a)`, 
      array('IP') AS `labels(a)`
FROM zeek.dns_log AS r
LIMIT 5