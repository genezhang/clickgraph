SELECT 
      CAST(u.full_name IS NOT NULL AS Nullable(Bool)) AS "has"
FROM test_integration.users_test AS u
