SELECT 
      v0.age AS `a.age`, 
      v0.city AS `a.city`, 
      v0.country AS `a.country`, 
      v0.email_address AS `a.email`, 
      v0.is_active AS `a.is_active`, 
      v0.full_name AS `a.name`, 
      v0.registration_date AS `a.registration_date`, 
      v0.user_id AS `a.user_id`
FROM test_integration.users_test AS v0
LIMIT 1
-- column a: Node { label: "User" } [("age", "a.age"), ("city", "a.city"), ("country", "a.country"), ("email", "a.email"), ("is_active", "a.is_active"), ("name", "a.name"), ("registration_date", "a.registration_date"), ("user_id", "a.user_id")]