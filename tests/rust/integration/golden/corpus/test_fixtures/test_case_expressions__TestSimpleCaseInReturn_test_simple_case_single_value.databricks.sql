SELECT 
      n.name AS `n.name`, 
      caseWithExpression(n.name, 'Alice', 'Admin', 'User') AS `role`
FROM test_integration.users AS n
ORDER BY n.name ASC
