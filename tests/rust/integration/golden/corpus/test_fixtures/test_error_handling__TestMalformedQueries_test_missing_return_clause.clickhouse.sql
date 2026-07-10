SELECT 
      a.age AS "age", 
      a.name AS "name", 
      a.user_id AS "user_id"
FROM test_integration.users AS a
WHERE a.age > 25
