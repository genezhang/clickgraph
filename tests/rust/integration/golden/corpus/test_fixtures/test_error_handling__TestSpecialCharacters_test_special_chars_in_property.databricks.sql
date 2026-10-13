SELECT 
      v0.name AS `a.name`
FROM test_integration.users AS v0
WHERE (position('@#$%', v0.name) > 0)

-- column a.name: Value [("a.name", "a.name")]