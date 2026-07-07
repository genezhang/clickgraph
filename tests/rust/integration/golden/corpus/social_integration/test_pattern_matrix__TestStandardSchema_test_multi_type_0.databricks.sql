SELECT 
      'FOLLOWS::User::User' AS `type(r)`, 
      count(*) AS `cnt`
FROM test_integration.user_follows_test AS r
GROUP BY 'FOLLOWS::User::User'
