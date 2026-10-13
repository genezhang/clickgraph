SELECT 
      'TestUser' AS `lbl_string`, 
      array('TestUser') AS `lbl_array`
FROM test_integration.users AS v0
LIMIT 1
-- column lbl_string: Value [("lbl_string", "lbl_string")]
-- column lbl_array: Value [("lbl_array", "lbl_array")]