SELECT 
      n.name AS `n.name`
FROM test_integration.users AS n
WHERE caseWithExpression(n.name, 'Alice', 1, 'Bob', 1, 0) = 1
ORDER BY n.name ASC
