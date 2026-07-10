SELECT 
      a.name AS `a.name`
FROM test_integration.users AS b
INNER JOIN test_integration.follows AS r2 ON r2.followed_id = b.user_id
INNER JOIN test_integration.follows AS r1 ON r1.followed_id = r2.follower_id
INNER JOIN test_integration.users AS a ON a.user_id = r1.follower_id
WHERE b.name = 'Diana'
ORDER BY a.name ASC
