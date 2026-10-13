SELECT 
      v0.name AS `a.name`, 
      v2.name AS `b.name`, 
      CASE WHEN v1.since > 2022 THEN 'Recent' WHEN v1.since > 2020 THEN 'Medium' ELSE 'Old' END AS `relationship_age`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
ORDER BY v0.name ASC NULLS LAST, v2.name ASC NULLS LAST

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]
-- column relationship_age: Value [("relationship_age", "relationship_age")]