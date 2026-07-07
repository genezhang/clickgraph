SELECT 
      r.follow_date AS `r.follow_date`, 
      neighbor.full_name AS `neighbor.name`
FROM test_integration.users_test AS u
INNER JOIN test_integration.user_follows_test AS r ON r.follower_id = u.user_id
INNER JOIN test_integration.users_test AS neighbor ON neighbor.user_id = r.followed_id
WHERE u.user_id = 1
LIMIT 5