SELECT 
      b.name AS `b.name`
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS r ON a.user_id = r.followed_id
INNER JOIN test_integration.users AS b ON r.follower_id = b.user_id
WHERE a.name = 'Charlie'
ORDER BY b.name ASC
