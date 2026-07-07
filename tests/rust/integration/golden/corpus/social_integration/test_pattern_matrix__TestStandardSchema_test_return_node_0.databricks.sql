SELECT 
      n.age AS `n.age`, 
      n.city AS `n.city`, 
      n.country AS `n.country`, 
      n.email_address AS `n.email`, 
      n.is_active AS `n.is_active`, 
      n.full_name AS `n.name`, 
      n.registration_date AS `n.registration_date`, 
      n.user_id AS `n.user_id`
FROM test_integration.users_test AS n
LIMIT 5