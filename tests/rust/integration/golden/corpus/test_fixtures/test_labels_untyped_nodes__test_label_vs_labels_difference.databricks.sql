SELECT 
      'TestUser' AS `lbl_string`, 
      array('TestUser') AS `lbl_array`
FROM test_integration.users AS u
LIMIT 1