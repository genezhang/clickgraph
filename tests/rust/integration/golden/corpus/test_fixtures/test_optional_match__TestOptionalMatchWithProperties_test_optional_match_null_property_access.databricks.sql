SELECT 
      a.name AS `a.name`, 
      b.name AS `b.name`, 
      b.age AS `b.age`
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = t0.followed_id
WHERE a.name = 'Eve'
