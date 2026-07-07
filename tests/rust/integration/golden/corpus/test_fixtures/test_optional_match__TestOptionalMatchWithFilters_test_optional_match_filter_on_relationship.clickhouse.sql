SELECT 
      a.name AS "a.name", 
      b.name AS "b.name"
FROM test_integration.users AS a
LEFT JOIN test_integration.follows AS r ON r.follower_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = r.followed_id
WHERE (r.since > '2020' AND a.name = 'Alice')
