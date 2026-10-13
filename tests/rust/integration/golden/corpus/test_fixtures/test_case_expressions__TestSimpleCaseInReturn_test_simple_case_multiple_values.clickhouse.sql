SELECT 
      v0.name AS "n.name", 
      caseWithExpression(v0.name, 'Alice', 'Level 3', 'Bob', 'Level 2', 'Charlie', 'Level 2', 'Level 1') AS "level"
FROM test_integration.users AS v0
ORDER BY v0.name ASC
