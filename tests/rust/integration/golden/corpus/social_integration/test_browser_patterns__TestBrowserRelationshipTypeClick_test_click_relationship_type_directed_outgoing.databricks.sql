SELECT 
      v1.follower_id AS `r.from_id`, 
      v1.followed_id AS `r.to_id`, 
      v1.follow_date AS `r.follow_date`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
LIMIT 25