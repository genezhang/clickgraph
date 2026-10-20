SELECT 
      v1.`id.orig_h` AS `r.from_id`, 
      v1.query AS `r.to_id`, 
      v1.`id.resp_h` AS `r.dns_server`, 
      v1.qtype_name AS `r.qtype`, 
      v1.rcode_name AS `r.rcode`, 
      v1.ts AS `r.timestamp`, 
      v1.uid AS `r.uid`, 
      concat('REQUESTED:', concat(CASE WHEN v1.uid IS NULL THEN '\\N' ELSE replace(replace(replace(CAST(v1.uid AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END), ':', CAST(v1.`id.orig_h` AS STRING), '->', CAST(v1.query AS STRING), '-') AS `r.elementId`
FROM zeek.dns_log AS v1
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL)
LIMIT 5
-- column r: Rel { rel_type: "REQUESTED", from_label: "IP", to_label: "Domain" } [("from_id", "r.from_id"), ("to_id", "r.to_id"), ("dns_server", "r.dns_server"), ("qtype", "r.qtype"), ("rcode", "r.rcode"), ("timestamp", "r.timestamp"), ("uid", "r.uid"), ("#elementId", "r.elementId")]