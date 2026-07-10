WITH with_a_follow_count_cte_0 AS (SELECT 
      any_value(a.name) AS `p1_a_name`, 
      count(t0.followed_id) AS `follow_count`
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
GROUP BY a.user_id
HAVING follow_count >= 1
)
SELECT 
      a_follow_count.p1_a_name AS `a.name`, 
      a_follow_count.follow_count AS `follow_count`
FROM with_a_follow_count_cte_0 AS a_follow_count
ORDER BY a_follow_count.p1_a_name ASC
