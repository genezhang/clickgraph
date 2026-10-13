SELECT 
      v0.name AS `a.name`, 
      v2.name AS `b.name`, 
      count(*) AS `connection_count`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
GROUP BY v0.name, v2.name
ORDER BY v0.name ASC NULLS LAST, v2.name ASC NULLS LAST

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]
-- column connection_count: Value [("connection_count", "connection_count")]