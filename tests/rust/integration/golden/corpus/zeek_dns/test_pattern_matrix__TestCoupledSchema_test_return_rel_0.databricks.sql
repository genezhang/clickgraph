SELECT 
      r.`id.orig_h` AS `r.from_id`, 
      r.query AS `r.to_id`, 
      r.`id.resp_h` AS `r.dns_server`, 
      r.qtype_name AS `r.qtype`, 
      r.rcode_name AS `r.rcode`, 
      r.ts AS `r.timestamp`, 
      r.uid AS `r.uid`
FROM zeek.dns_log AS r
LIMIT 5