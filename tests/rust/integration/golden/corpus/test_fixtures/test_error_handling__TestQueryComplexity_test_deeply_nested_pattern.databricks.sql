SELECT 
      v0.name AS `a.name`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
JOIN test_integration.follows AS v3 ON v3.follower_id = v2.user_id
JOIN test_integration.users AS v4 ON v3.followed_id = v4.user_id
JOIN test_integration.follows AS v5 ON v5.follower_id = v4.user_id
JOIN test_integration.users AS v6 ON v5.followed_id = v6.user_id
JOIN test_integration.follows AS v7 ON v7.follower_id = v6.user_id
JOIN test_integration.users AS v8 ON v7.followed_id = v8.user_id
JOIN test_integration.follows AS v9 ON v9.follower_id = v8.user_id
JOIN test_integration.users AS v10 ON v9.followed_id = v10.user_id
JOIN test_integration.follows AS v11 ON v11.follower_id = v10.user_id
JOIN test_integration.users AS v12 ON v11.followed_id = v12.user_id
WHERE ((v1.follower_id <> v3.follower_id OR v1.followed_id <> v3.followed_id) AND (v1.follower_id <> v5.follower_id OR v1.followed_id <> v5.followed_id) AND (v1.follower_id <> v7.follower_id OR v1.followed_id <> v7.followed_id) AND (v1.follower_id <> v9.follower_id OR v1.followed_id <> v9.followed_id) AND (v1.follower_id <> v11.follower_id OR v1.followed_id <> v11.followed_id) AND (v3.follower_id <> v5.follower_id OR v3.followed_id <> v5.followed_id) AND (v3.follower_id <> v7.follower_id OR v3.followed_id <> v7.followed_id) AND (v3.follower_id <> v9.follower_id OR v3.followed_id <> v9.followed_id) AND (v3.follower_id <> v11.follower_id OR v3.followed_id <> v11.followed_id) AND (v5.follower_id <> v7.follower_id OR v5.followed_id <> v7.followed_id) AND (v5.follower_id <> v9.follower_id OR v5.followed_id <> v9.followed_id) AND (v5.follower_id <> v11.follower_id OR v5.followed_id <> v11.followed_id) AND (v7.follower_id <> v9.follower_id OR v7.followed_id <> v9.followed_id) AND (v7.follower_id <> v11.follower_id OR v7.followed_id <> v11.followed_id) AND (v9.follower_id <> v11.follower_id OR v9.followed_id <> v11.followed_id))

-- column a.name: Value [("a.name", "a.name")]