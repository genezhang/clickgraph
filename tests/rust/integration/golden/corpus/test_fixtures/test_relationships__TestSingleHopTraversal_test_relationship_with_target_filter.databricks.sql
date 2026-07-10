SELECT 
      a.name AS `a.name`
FROM test_integration.users AS b
INNER JOIN test_integration.follows AS r ON b.user_id = r.followed_id
INNER JOIN test_integration.users AS a ON r.follower_id = a.user_id
WHERE b.name = 'Diana'
ORDER BY a.name ASC
