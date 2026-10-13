SELECT 
      v0.name AS "n.name"
FROM test_integration.users AS v0
WHERE caseWithExpression(v0.name, 'Alice', 1, 'Bob', 1, 0) = 1
ORDER BY v0.name ASC

-- column n.name: Value [("n.name", "n.name")]