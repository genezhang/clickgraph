SELECT 
      n.is_active AS `n.is_active`
FROM test_integration.users_test AS n
WHERE n.is_active IS NOT NULL
LIMIT 10