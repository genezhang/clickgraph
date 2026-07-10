SELECT DISTINCT 
      a.name AS "a.name", 
      b.name AS "b.name"
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = t0.followed_id
WHERE ((a.name = 'Alice' OR a.name = 'Bob') AND a.user_id IN [1, 2])
ORDER BY a.name ASC, b.name ASC
