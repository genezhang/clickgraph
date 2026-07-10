SELECT 
      count(t0.followed_id) AS `count`
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
WHERE a.name = 'NonExistent'
