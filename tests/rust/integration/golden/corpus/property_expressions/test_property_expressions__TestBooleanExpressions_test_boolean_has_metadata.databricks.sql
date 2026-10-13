SELECT 
      count(*) AS `with_metadata`
FROM test_integration.users_expressions_test AS v0
WHERE (length(v0.metadata_json) > 2) = true

-- column with_metadata: Value [("with_metadata", "with_metadata")]