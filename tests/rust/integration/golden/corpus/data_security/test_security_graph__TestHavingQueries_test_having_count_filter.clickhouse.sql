WITH with_cnt_g_cte_0 AS (SELECT 
      anyLast(g.name) AS "p1_g_name", 
      count(u.user_id) AS "cnt"
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = u.user_id AND t0.member_type = 'User'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
GROUP BY g.group_id
HAVING cnt >= 2
)
SELECT 
      cnt_g.p1_g_name AS "g.name", 
      cnt_g.cnt AS "cnt"
FROM with_cnt_g_cte_0 AS cnt_g
