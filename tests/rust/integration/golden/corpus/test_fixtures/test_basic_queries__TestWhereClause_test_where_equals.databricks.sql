SELECT 
      v0.name AS `u.name`, 
      v0.age AS `u.age`
FROM test_integration.users AS v0
WHERE v0.name = 'Alice'

-- column u.name: Value [("u.name", "u.name")]
-- column u.age: Value [("u.age", "u.age")]