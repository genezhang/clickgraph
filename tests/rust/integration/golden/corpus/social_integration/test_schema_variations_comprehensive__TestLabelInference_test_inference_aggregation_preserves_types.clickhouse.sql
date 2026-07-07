WITH with_a_post_count_cte_0 AS (SELECT 
      b.author_id AS "p1_a_user_id"
FROM test_integration.posts_test AS b
GROUP BY b.author_id
)
SELECT 
      count(*) AS "total"
FROM test_integration.user_follows_test AS t0
INNER JOIN with_a_post_count_cte_0 AS a_post_count ON 1 = 1
INNER JOIN with_a_post_count_cte_0 AS a ON t0.follower_id = a_post_count.p1_a_user_id
