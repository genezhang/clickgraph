SELECT `lbl` AS `lbl` FROM (
SELECT DISTINCT 
      'TestProduct' AS "lbl", 
      lbl AS "__order_col_0"
FROM test_integration.products AS n
UNION ALL 
SELECT DISTINCT 
      'TestUser' AS "lbl", 
      lbl AS "__order_col_0"
FROM test_integration.users AS n
) AS __union
ORDER BY __union.`__order_col_0` ASC
