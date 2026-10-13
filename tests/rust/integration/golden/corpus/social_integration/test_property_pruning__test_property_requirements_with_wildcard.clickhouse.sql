SELECT 
      v0.age AS "u.age", 
      v0.city AS "u.city", 
      v0.country AS "u.country", 
      v0.email_address AS "u.email", 
      v0.is_active AS "u.is_active", 
      v0.full_name AS "u.name", 
      v0.registration_date AS "u.registration_date", 
      v0.user_id AS "u.user_id"
FROM test_integration.users_test AS v0
WHERE v0.user_id = 1
