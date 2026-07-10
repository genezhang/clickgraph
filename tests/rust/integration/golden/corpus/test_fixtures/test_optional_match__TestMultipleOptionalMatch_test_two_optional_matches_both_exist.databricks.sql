SELECT 
      a.name AS `a.name`, 
      b.name AS `b.name`, 
      c.name AS `c.name`
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = t0.followed_id
LEFT JOIN test_integration.follows AS t1 ON t1.follower_id = a.user_id
LEFT JOIN test_integration.users AS c ON c.user_id = t1.followed_id
WHERE (c.name <> b.name AND a.name = 'Alice')
ORDER BY b.name ASC, c.name ASC
