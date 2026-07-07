SELECT 
      count(*) AS "with_metadata"
FROM test_integration.users_expressions_test AS u
WHERE (length(u.metadata_json) > 2) = true
