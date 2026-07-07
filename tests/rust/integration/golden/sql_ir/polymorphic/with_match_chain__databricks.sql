WITH with_u_cte_0 AS (SELECT 
      u.full_name AS `p1_u_name`, 
      u.user_id AS `p1_u_user_id`
FROM brahmand.users_bench AS u
WHERE u.user_id > 2
)
SELECT 
      u.p1_u_name AS `u.name`, 
      f.full_name AS `f.name`
FROM with_u_cte_0 AS u
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = u.p1_u_user_id AND (t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User')
INNER JOIN brahmand.users_bench AS f ON f.user_id = t0.to_id
