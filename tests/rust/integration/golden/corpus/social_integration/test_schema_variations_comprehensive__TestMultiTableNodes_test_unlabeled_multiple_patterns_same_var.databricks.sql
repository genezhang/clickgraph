SELECT 
      count(*) AS `total`
FROM test_integration.users_test AS v0
JOIN test_integration.posts_test AS v1 ON v1.author_id = v0.user_id
JOIN test_integration.posts_test AS v2 ON v1.post_id = v2.post_id
JOIN test_integration.user_follows_test AS v3 ON v3.follower_id = v0.user_id
JOIN test_integration.users_test AS v4 ON v3.followed_id = v4.user_id

-- column total: Value [("total", "total")]