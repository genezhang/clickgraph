SELECT 
      count(*) AS `count(*)`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id AND v1.followed_id = v0.user_id
