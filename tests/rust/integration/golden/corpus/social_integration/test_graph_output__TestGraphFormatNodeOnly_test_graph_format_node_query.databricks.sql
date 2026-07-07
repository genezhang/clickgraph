SELECT 
      u.age AS `u.age`, 
      u.city AS `u.city`, 
      u.country AS `u.country`, 
      u.email_address AS `u.email`, 
      u.is_active AS `u.is_active`, 
      u.full_name AS `u.name`, 
      u.registration_date AS `u.registration_date`, 
      u.user_id AS `u.user_id`
FROM test_integration.users_test AS u
LIMIT 5