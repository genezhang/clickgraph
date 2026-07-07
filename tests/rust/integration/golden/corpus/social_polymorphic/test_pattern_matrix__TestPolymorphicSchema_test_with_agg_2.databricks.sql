WITH with_cnt_prop_cte_0 AS (SELECT 
      a.full_name AS `prop`, 
      count(*) AS `cnt`
FROM brahmand.users_bench AS a
INNER JOIN brahmand.interactions AS r ON r.from_id = a.user_id AND r.interaction_type = 'LIKES' AND r.from_type = 'User' AND r.to_type = 'User'
GROUP BY a.full_name
)
SELECT 
      cnt_prop.prop AS `prop`, 
      cnt_prop.cnt AS `cnt`
FROM with_cnt_prop_cte_0 AS cnt_prop
ORDER BY cnt_prop.cnt DESC
LIMIT 10