WITH with_a_follows_cte_0 AS (SELECT 
      count(t0.followed_id) AS `follows`
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
GROUP BY a.user_id
)
SELECT 
      avg(a_follows.follows) AS `avg_follows_per_user`
FROM with_a_follows_cte_0 AS a_follows
