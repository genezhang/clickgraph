WITH with_g_user_count_cte_0 AS (SELECT 
      any_value(g.name) AS `p1_g_name`, 
      count(u.user_id) AS `user_count`
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = u.user_id AND t0.member_type = 'User'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
GROUP BY g.group_id
HAVING user_count >= 1
)
SELECT 
      g_user_count.p1_g_name AS `g.name`, 
      g_user_count.user_count AS `user_count`
FROM with_g_user_count_cte_0 AS g_user_count
ORDER BY g_user_count.user_count DESC
