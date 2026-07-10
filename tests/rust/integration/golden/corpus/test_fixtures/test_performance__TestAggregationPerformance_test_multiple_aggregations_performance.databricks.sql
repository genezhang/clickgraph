SELECT 
      count(n.user_id) AS `total`, 
      avg(n.age) AS `avg_age`, 
      min(n.age) AS `min_age`, 
      max(n.age) AS `max_age`
FROM test_integration.users AS n
