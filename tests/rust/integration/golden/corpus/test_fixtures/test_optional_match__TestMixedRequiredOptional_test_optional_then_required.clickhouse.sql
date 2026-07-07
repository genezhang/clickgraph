SELECT 
      a.name AS "a.name", 
      b.name AS "b.name", 
      x.name AS "x.name"
FROM test_integration.users AS x
LEFT JOIN test_integration.users AS a ON 1 = 1
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = t0.followed_id
WHERE (x.name = 'Alice' AND a.name = 'Eve')
