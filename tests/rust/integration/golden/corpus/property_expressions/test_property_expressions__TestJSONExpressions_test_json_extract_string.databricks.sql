SELECT 
      u.user_id AS `u.user_id`, 
      concat(u.first_name, ' ', u.last_name) AS `u.full_name`, 
      JSONExtractString(u.metadata_json, 'subscription_type') AS `u.metadata_key`
FROM test_integration.users_expressions_test AS u
WHERE JSONExtractString(u.metadata_json, 'subscription_type') = 'premium'
ORDER BY u.user_id ASC
