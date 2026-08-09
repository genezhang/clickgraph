SELECT 
      minOrNull(n.age) AS "youngest", 
      maxOrNull(n.age) AS "oldest"
FROM test_integration.users AS n
