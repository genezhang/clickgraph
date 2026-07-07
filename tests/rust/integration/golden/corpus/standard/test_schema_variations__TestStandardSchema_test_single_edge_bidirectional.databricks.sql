SELECT `neighbor.user_id` AS `neighbor.user_id`, `neighbor.name` AS `neighbor.name` FROM (
SELECT 
      neighbor.user_id AS `neighbor.user_id`, 
      neighbor.full_name AS `neighbor.name`
FROM test_integration.users_test AS u
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = u.user_id
INNER JOIN test_integration.users_test AS neighbor ON neighbor.user_id = t0.followed_id
WHERE u.user_id = 1
UNION ALL 
SELECT 
      neighbor.user_id AS `neighbor.user_id`, 
      neighbor.full_name AS `neighbor.name`
FROM test_integration.users_test AS u
INNER JOIN test_integration.user_follows_test AS t0 ON u.user_id = t0.followed_id
INNER JOIN test_integration.users_test AS neighbor ON t0.follower_id = neighbor.user_id
WHERE u.user_id = 1
) AS __union
LIMIT 10