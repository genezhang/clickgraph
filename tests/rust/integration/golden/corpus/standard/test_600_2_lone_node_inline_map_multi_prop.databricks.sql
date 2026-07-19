SELECT 
      count(*) AS `count(*)`
FROM test_integration.users_test AS a
WHERE (a.full_name = 'Alice Johnson' AND a.country = 'USA')
