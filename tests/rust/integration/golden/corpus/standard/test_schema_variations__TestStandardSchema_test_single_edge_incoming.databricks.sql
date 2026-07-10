SELECT 
      follower.user_id AS `follower.user_id`, 
      follower.full_name AS `follower.name`
FROM test_integration.user_follows_test AS t0
INNER JOIN test_integration.users_test AS follower ON t0.follower_id = follower.user_id
WHERE t0.followed_id = 2
LIMIT 10