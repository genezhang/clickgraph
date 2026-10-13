SELECT 
      v2.user_id AS "u2.user_id", 
      count(v4.post_id) AS "post_count"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.posts_test AS v3 ON v3.author_id = v2.user_id
JOIN test_integration.posts_test AS v4 ON v3.post_id = v4.post_id
WHERE v0.user_id = 1
GROUP BY v2.user_id
LIMIT 10