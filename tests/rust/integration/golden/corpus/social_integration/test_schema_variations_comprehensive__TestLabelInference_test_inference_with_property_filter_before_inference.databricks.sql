SELECT 
      count(*) AS `total`
FROM test_integration.user_follows_test AS r
WHERE a.user_id = 1
