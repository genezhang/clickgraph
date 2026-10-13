SELECT 
      v0.name AS `a.name`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE v2.name = 'Diana'
ORDER BY v0.name ASC NULLS LAST

-- column a.name: Value [("a.name", "a.name")]