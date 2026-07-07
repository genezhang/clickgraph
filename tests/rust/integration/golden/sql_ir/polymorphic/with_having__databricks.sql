WITH with_n_name_cte_0 AS (SELECT 
      u.full_name AS `name`, 
      count(t0.to_id) AS `n`
FROM brahmand.users_bench AS u
INNER JOIN brahmand.interactions AS t0 ON t0.from_id = u.user_id AND t0.interaction_type = 'FOLLOWS' AND t0.from_type = 'User' AND t0.to_type = 'User'
GROUP BY u.full_name
HAVING n > 1
)
SELECT 
      n_name.name AS `name`, 
      n_name.n AS `n`
FROM with_n_name_cte_0 AS n_name
