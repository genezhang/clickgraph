SELECT 
      v1.query AS `r.fromId`, 
      v1.answers AS `r.toId`, 
      v1.query AS `r.query`, 
      v1.ts AS `r.timestamp`, 
      v1.uid AS `r.uid`, 
      concat('RESOLVED_TO:', concat(CASE WHEN v1.uid IS NULL THEN '\\N' ELSE replace(replace(replace(CAST(v1.uid AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END, '|', CASE WHEN v1.answers IS NULL THEN '\\N' ELSE replace(replace(replace(CAST(v1.answers AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END), ':', CAST(v1.query AS STRING), '->', CAST(v1.answers AS STRING), '-') AS `r.elementId`
FROM zeek.dns_log AS v1
WHERE (v1.query IS NOT NULL AND v1.answers IS NOT NULL)
LIMIT 5
-- column r: Rel { rel_type: "RESOLVED_TO", from_label: "Domain", to_label: "ResolvedIP" } [("#fromId", "r.fromId"), ("#toId", "r.toId"), ("query", "r.query"), ("timestamp", "r.timestamp"), ("uid", "r.uid"), ("#elementId", "r.elementId")]