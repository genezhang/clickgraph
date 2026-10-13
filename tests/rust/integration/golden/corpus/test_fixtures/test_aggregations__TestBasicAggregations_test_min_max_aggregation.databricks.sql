SELECT 
      min(v0.age) AS `youngest`, 
      max(v0.age) AS `oldest`
FROM test_integration.users AS v0

-- column youngest: Value [("youngest", "youngest")]
-- column oldest: Value [("oldest", "oldest")]