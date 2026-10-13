WITH optional_o1 AS (
SELECT 
      v3.follower_id AS "v0__user_id", 
      v4.user_id AS "v4__user_id"
FROM test_integration.user_follows_test AS v3
JOIN test_integration.users_test AS v4 ON v3.followed_id = v4.user_id
)
SELECT 
      count(*) AS "total"
FROM test_integration.users_test AS v0
JOIN test_integration.posts_test AS v1 ON v1.author_id = v0.user_id
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id

-- column total: Value [("total", "total")]