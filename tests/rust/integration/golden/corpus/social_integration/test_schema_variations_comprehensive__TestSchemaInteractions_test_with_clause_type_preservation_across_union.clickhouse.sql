WITH with_a_like_count_p_cte_0 AS (SELECT 
      p.author_id AS "p1_a_user_id"
FROM test_integration.posts_test AS p
)
SELECT 
      count(*) AS "total"
FROM test_integration.user_follows_test AS t0
INNER JOIN with_a_like_count_p_cte_0 AS a_like_count_p ON 1 = 1
INNER JOIN with_a_like_count_p_cte_0 AS a ON t0.follower_id = a_like_count_p.p1_a_user_id
