SELECT 
      v3.post_id AS `r.post_id`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.posts_test AS v3 ON v3.author_id = v2.user_id
JOIN test_integration.posts_test AS v4 ON v3.post_id = v4.post_id
ORDER BY v3.post_id ASC NULLS LAST

-- column r.post_id: Value [("r.post_id", "r.post_id")]