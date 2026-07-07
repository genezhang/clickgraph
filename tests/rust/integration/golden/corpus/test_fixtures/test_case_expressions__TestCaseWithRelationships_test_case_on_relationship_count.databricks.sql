WITH with_a_follows_cte_0 AS (SELECT 
      any_value(a.name) AS `p1_a_name`, 
      count(t0.followed_id) AS `follows`
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
GROUP BY a.user_id
)
SELECT 
      a_follows.p1_a_name AS `a.name`, 
      CASE WHEN a_follows.follows = 0 THEN 'No follows' WHEN a_follows.follows = 1 THEN 'One follow' ELSE 'Multiple follows' END AS `follow_status`
FROM with_a_follows_cte_0 AS a_follows
ORDER BY a_follows.p1_a_name ASC
