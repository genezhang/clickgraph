SELECT 
      v0.name AS "a.name", 
      v2.name AS "b.name"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE v1.since >= '2023-02-01'
ORDER BY v0.name ASC, v2.name ASC

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]