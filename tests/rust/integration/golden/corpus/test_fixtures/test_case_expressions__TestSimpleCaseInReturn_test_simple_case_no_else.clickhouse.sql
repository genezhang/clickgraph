SELECT 
      v0.name AS "n.name", 
      caseWithExpression(v0.name, 'Alice', 'VIP', NULL) AS "status"
FROM test_integration.users AS v0
ORDER BY v0.name ASC

-- column n.name: Value [("n.name", "n.name")]
-- column status: Value [("status", "status")]