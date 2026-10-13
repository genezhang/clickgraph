SELECT 
      v0.name AS `u.name`, 
      v0.age AS `u.age`
FROM test_integration.users AS v0
WHERE v0.age > 30
ORDER BY v0.age ASC NULLS LAST
