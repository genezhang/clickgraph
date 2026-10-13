SELECT 
      CAST(v0.full_name IS NOT NULL AS Nullable(Bool)) AS "has"
FROM test_integration.users_test AS v0

-- column has: Value [("has", "has")]