WITH with_w1 AS (
SELECT 
      v0.user_id AS "v3__user_id", 
      v2.post_id AS "v4__post_id", 
      v2.likes AS "v5"
FROM test_integration.users_test AS v0
JOIN test_integration.posts_test AS v1 ON v1.author_id = v0.user_id
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
)
SELECT 
      count(*) AS "total"
FROM with_w1 AS w1
JOIN test_integration.user_follows_test AS v6 ON v6.follower_id = w1.v3__user_id
JOIN test_integration.users_test AS v7 ON v6.followed_id = v7.user_id
