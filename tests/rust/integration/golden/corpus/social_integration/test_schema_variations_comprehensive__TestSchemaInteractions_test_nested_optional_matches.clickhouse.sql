WITH optional_o1 AS (
SELECT 
      v1.author_id AS "v0__user_id", 
      v2.post_id AS "v2__post_id"
FROM test_integration.posts_test AS v1
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
), 
optional_o2 AS (
SELECT 
      v3.post_id AS "v2__post_id", 
      v4.user_id AS "v4__user_id"
FROM test_integration.post_likes_test AS v3
JOIN test_integration.users_test AS v4 ON v3.user_id = v4.user_id
WHERE v3.post_id IS NOT NULL
)
SELECT 
      count(*) AS "total"
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
LEFT JOIN optional_o2 AS o2 ON o1.v2__post_id = o2.v2__post_id

-- column total: Value [("total", "total")]