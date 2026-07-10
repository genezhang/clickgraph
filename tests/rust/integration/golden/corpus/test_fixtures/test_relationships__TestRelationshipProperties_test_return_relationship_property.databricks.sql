SELECT 
      a.name AS `a.name`, 
      b.name AS `b.name`, 
      r.since AS `r.since`
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS r ON r.follower_id = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = r.followed_id
WHERE a.name = 'Alice'
ORDER BY b.name ASC
