SELECT 
      a.name AS `a.name`
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
ORDER BY a.name ASC
