SELECT 
      n.full_name AS "n.name"
FROM test_integration.users_test AS n
WHERE n.full_name IS NOT NULL
LIMIT 10