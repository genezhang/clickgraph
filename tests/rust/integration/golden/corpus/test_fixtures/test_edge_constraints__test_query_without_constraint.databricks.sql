SELECT 
      v0.name AS `a.name`, 
      v2.name AS `b.name`
FROM test_integration.users AS v0
JOIN test_integration.friendships AS v1 ON v1.user_id_1 = v0.user_id
JOIN test_integration.users AS v2 ON v1.user_id_2 = v2.user_id
WHERE v0.name = 'Alice'

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]