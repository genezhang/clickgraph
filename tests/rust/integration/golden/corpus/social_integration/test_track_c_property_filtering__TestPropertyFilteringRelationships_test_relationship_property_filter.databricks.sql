SELECT 
      r.follow_date AS `date`
FROM test_integration.user_follows_test AS r
WHERE r.follow_date IS NOT NULL
LIMIT 5