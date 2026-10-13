SELECT 
      v0.name AS "n.name", 
      v0.age AS "n.age"
FROM test_integration.users AS v0
WHERE CASE WHEN v0.age >= 30 THEN true ELSE false END
ORDER BY v0.name ASC
