SELECT 
      v1.query AS "r.fromId", 
      v1.answers AS "r.toId", 
      v1.query AS "r.query", 
      v1.ts AS "r.timestamp", 
      v1.uid AS "r.uid", 
      concat('RESOLVED_TO:', concat(CASE WHEN v1.uid IS NULL THEN '\\N' ELSE replaceAll(replaceAll(replaceAll(toString(v1.uid), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END, '|', CASE WHEN v1.answers IS NULL THEN '\\N' ELSE replaceAll(replaceAll(replaceAll(toString(v1.answers), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END), ':', toString(v1.query), '->', toString(v1.answers), '-') AS "r.elementId"
FROM zeek.dns_log AS v1
WHERE (v1.query IS NOT NULL AND v1.answers IS NOT NULL)
LIMIT 5
-- column r: Rel { rel_type: "RESOLVED_TO", from_label: "Domain", to_label: "ResolvedIP" } [("#fromId", "r.fromId"), ("#toId", "r.toId"), ("query", "r.query"), ("timestamp", "r.timestamp"), ("uid", "r.uid"), ("#elementId", "r.elementId")]