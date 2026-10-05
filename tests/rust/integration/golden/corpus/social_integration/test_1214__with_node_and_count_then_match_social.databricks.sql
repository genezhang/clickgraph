WITH with_a_n_cte_0 AS (SELECT 
      b.author_id AS `p1_a_user_id`
FROM test_integration.posts_test AS b
GROUP BY b.author_id
)
SELECT 
      count(*) AS `count(*)`
FROM test_integration.user_follows_test AS t0
INNER JOIN with_a_n_cte_0 AS a ON t0.follower_id = a.p1_a_user_id
