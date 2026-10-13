SELECT 
      v0.age AS "n.age", 
      v0.city AS "n.city", 
      v0.country AS "n.country", 
      v0.email_address AS "n.email", 
      v0.is_active AS "n.is_active", 
      v0.full_name AS "n.name", 
      v0.registration_date AS "n.registration_date", 
      v0.user_id AS "n.user_id"
FROM test_integration.users_test AS v0
LIMIT 5