SELECT 
      v0.name AS "n.name", 
      v0.age AS "n.age", 
      CASE WHEN v0.age >= 30 THEN caseWithExpression(v0.name, 'Alice', 'Senior Admin', 'Senior User') ELSE CASE WHEN v0.age >= 25 THEN 'Regular User' ELSE 'Junior User' END END AS "role"
FROM test_integration.users AS v0
ORDER BY v0.name ASC
