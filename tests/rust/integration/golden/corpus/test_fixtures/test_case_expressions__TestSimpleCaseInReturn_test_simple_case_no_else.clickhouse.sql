SELECT 
      n.name AS "n.name", 
      caseWithExpression(n.name, 'Alice', 'VIP', NULL) AS "status"
FROM test_integration.users AS n
ORDER BY n.name ASC
