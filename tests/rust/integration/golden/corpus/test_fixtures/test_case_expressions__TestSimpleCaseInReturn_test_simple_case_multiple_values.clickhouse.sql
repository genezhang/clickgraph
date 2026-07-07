SELECT 
      n.name AS "n.name", 
      caseWithExpression(n.name, 'Alice', 'Level 3', 'Bob', 'Level 2', 'Charlie', 'Level 2', 'Level 1') AS "level"
FROM test_integration.users AS n
ORDER BY n.name ASC
