SELECT 
      v0.user_id AS "u.user_id", 
      concat(v0.first_name, ' ', v0.last_name) AS "u.full_name", 
      JSONExtractString(v0.metadata_json, 'subscription_type') AS "u.metadata_key"
FROM test_integration.users_expressions_test AS v0
WHERE JSONExtractString(v0.metadata_json, 'subscription_type') = 'premium'
ORDER BY v0.user_id ASC

-- column u.user_id: Value [("u.user_id", "u.user_id")]
-- column u.full_name: Value [("u.full_name", "u.full_name")]
-- column u.metadata_key: Value [("u.metadata_key", "u.metadata_key")]