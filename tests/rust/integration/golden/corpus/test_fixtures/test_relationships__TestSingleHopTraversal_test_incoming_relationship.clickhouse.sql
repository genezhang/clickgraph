SELECT 
      v2.name AS "b.name"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.followed_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.follower_id = v2.user_id
WHERE v0.name = 'Charlie'
ORDER BY v2.name ASC
