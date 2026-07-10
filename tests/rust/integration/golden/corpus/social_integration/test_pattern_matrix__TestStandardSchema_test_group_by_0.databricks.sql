SELECT 
      n.is_active AS `n.is_active`, 
      count(n.user_id) AS `cnt`
FROM test_integration.users_test AS n
GROUP BY n.is_active
ORDER BY cnt DESC
LIMIT 10