SELECT 
      v1.from_id AS `r.from_id`, 
      v1.to_id AS `r.to_id`, 
      v1.timestamp AS `r.created_at`, 
      v1.interaction_weight AS `r.weight`
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.to_id = v2.user_id
WHERE ((((v1.`interaction_type` = 'LIKES') AND (v1.`from_type` = 'User')) AND (v1.`to_type` = 'User')))
LIMIT 5