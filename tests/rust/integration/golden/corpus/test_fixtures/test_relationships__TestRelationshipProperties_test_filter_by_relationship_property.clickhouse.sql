SELECT 
      a.name AS "a.name", 
      b.name AS "b.name"
FROM test_integration.follows AS r
INNER JOIN test_integration.users AS b ON b.user_id = r.followed_id
INNER JOIN test_integration.users AS a ON r.follower_id = a.user_id
WHERE r.since >= '2023-02-01'
ORDER BY a.name ASC, b.name ASC
