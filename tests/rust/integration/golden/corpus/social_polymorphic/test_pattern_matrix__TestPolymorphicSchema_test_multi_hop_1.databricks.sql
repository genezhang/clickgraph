SELECT 
      v0.full_name AS `a.name`, 
      v4.full_name AS `c.name`
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.to_id = v2.user_id
JOIN brahmand.interactions AS v3 ON v3.from_id = v2.user_id
JOIN brahmand.users_bench AS v4 ON v3.to_id = v4.user_id
WHERE (((((v1.`interaction_type` = 'FOLLOWS') AND (v1.`from_type` = 'User')) AND (v1.`to_type` = 'User'))) AND ((((v3.`interaction_type` = 'LIKES') AND (v3.`from_type` = 'User')) AND (v3.`to_type` = 'User'))))
LIMIT 5
-- column a.name: Value [("a.name", "a.name")]
-- column c.name: Value [("c.name", "c.name")]