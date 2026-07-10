WITH with_cnt_prop_cte_0 AS (SELECT 
      a.email_address AS `prop`, 
      count(r.from_id) AS `cnt`
FROM brahmand.users_bench AS a
INNER JOIN brahmand.interactions AS r ON r.from_id = a.user_id AND r.interaction_type = 'LIKES' AND r.from_type = 'User' AND r.to_type = 'User'
GROUP BY a.email_address
)
SELECT 
      cnt_prop.prop AS `prop`, 
      cnt_prop.cnt AS `cnt`
FROM with_cnt_prop_cte_0 AS cnt_prop
ORDER BY cnt_prop.cnt DESC
LIMIT 10