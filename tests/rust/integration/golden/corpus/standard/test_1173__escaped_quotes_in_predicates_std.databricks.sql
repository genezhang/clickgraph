SELECT 
      v0.full_name AS `u.name`
FROM test_integration.users_test AS v0
WHERE (startswith(v0.full_name, 'O\'') OR v0.full_name IN ('a\'b', 'say "hi"'))
