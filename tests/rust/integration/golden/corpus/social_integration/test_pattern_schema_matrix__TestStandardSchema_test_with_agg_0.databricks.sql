WITH with_w1 AS (
SELECT 
      v0.user_id AS `v3`, 
      count(v1.like_id) AS `v4`
FROM test_integration.users_test AS v0
JOIN test_integration.post_likes_test AS v1 ON v1.user_id = v0.user_id
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
GROUP BY v0.user_id
)
SELECT 
      w1.v3 AS `prop`, 
      w1.v4 AS `cnt`
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST
LIMIT 10