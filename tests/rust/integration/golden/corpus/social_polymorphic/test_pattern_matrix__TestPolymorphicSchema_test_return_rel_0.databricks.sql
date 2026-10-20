SELECT 
      v1.from_id AS `r.from_id`, 
      v1.to_id AS `r.to_id`, 
      v1.timestamp AS `r.created_at`, 
      v1.interaction_weight AS `r.weight`, 
      concat('FOLLOWS:', concat(CASE WHEN v1.from_id IS NULL THEN '\\N' ELSE replace(replace(replace(CAST(v1.from_id AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END, '|', CASE WHEN v1.to_id IS NULL THEN '\\N' ELSE replace(replace(replace(CAST(v1.to_id AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END, '|', CASE WHEN v1.interaction_type IS NULL THEN '\\N' ELSE replace(replace(replace(CAST(v1.interaction_type AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END, '|', CASE WHEN v1.timestamp IS NULL THEN '\\N' ELSE replace(replace(replace(CAST(v1.timestamp AS STRING), '\\', '\\\\'), ':', '\\:'), '|', '\\|') END), ':', CAST(v1.from_id AS STRING), '->', CAST(v1.to_id AS STRING), '-') AS `r.elementId`
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.to_id = v2.user_id
WHERE ((((v1.`interaction_type` = 'FOLLOWS') AND (v1.`from_type` = 'User')) AND (v1.`to_type` = 'User')))
LIMIT 5
-- column r: Rel { rel_type: "FOLLOWS", from_label: "User", to_label: "User" } [("from_id", "r.from_id"), ("to_id", "r.to_id"), ("created_at", "r.created_at"), ("weight", "r.weight"), ("#elementId", "r.elementId")]