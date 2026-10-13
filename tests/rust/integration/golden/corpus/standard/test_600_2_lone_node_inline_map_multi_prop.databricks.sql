SELECT 
      count(*) AS `count(*)`
FROM test_integration.users_test AS v0
WHERE (v0.full_name = 'Alice Johnson' AND v0.country = 'USA')
