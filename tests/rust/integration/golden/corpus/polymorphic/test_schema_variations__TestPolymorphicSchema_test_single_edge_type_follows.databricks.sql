SELECT 
      v2.user_id AS `neighbor.user_id`, 
      v2.full_name AS `neighbor.name`
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.to_id = v2.user_id
WHERE (((((v1.`interaction_type` = 'FOLLOWS') AND (v1.`from_type` = 'User')) AND (v1.`to_type` = 'User'))) AND v0.user_id = 1)
LIMIT 10