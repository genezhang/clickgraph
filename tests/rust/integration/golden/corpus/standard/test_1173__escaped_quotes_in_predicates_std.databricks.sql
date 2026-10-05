SELECT 
      u.full_name AS `u.name`
FROM test_integration.users_test AS u
WHERE (startswith(u.full_name, 'O''') OR u.full_name IN ('a''b', 'say "hi"'))
