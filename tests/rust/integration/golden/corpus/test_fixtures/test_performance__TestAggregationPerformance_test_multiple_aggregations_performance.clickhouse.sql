SELECT 
      count(n.user_id) AS "total", 
      avg(n.age) AS "avg_age", 
      minOrNull(n.age) AS "min_age", 
      maxOrNull(n.age) AS "max_age"
FROM test_integration.users AS n
