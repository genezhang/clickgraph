SELECT 
      count(*) AS "bronze_count"
FROM test_integration.users_expressions_test AS u
WHERE if((u.score >= 1000), 'gold', if((u.score >= 500), 'silver', 'bronze')) = 'bronze'
