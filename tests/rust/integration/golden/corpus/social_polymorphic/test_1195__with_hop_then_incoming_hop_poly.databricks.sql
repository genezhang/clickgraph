WITH with_w1 AS (
SELECT 
      v2.user_id AS `v3__user_id`, 
      v2.user_id AS `p2_v3_user_id`
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.to_id = v2.user_id
WHERE ((((v1.`interaction_type` = 'FOLLOWS') AND (v1.`from_type` = 'User')) AND (v1.`to_type` = 'User')))
)
SELECT 
      w1.p2_v3_user_id AS `c.user_id`, 
      v5.user_id AS `n1.user_id`, 
      v7.user_id AS `n2.user_id`
FROM with_w1 AS w1
JOIN brahmand.interactions AS v4 ON v4.from_id = w1.v3__user_id
JOIN brahmand.users_bench AS v5 ON v4.to_id = v5.user_id
JOIN brahmand.interactions AS v6 ON v6.to_id = v5.user_id
JOIN brahmand.users_bench AS v7 ON v6.from_id = v7.user_id
WHERE (((((v4.`interaction_type` = 'FOLLOWS') AND (v4.`from_type` = 'User')) AND (v4.`to_type` = 'User'))) AND ((((v6.`interaction_type` = 'FOLLOWS') AND (v6.`from_type` = 'User')) AND (v6.`to_type` = 'User'))) AND (v4.from_id <> v6.from_id OR v4.to_id <> v6.to_id OR v4.interaction_type <> v6.interaction_type OR v4.timestamp <> v6.timestamp))
