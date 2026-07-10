SELECT `label` AS `label` FROM (
SELECT DISTINCT 
      array('TestProduct') AS `label`, 
      label AS `__order_col_0`
FROM test_integration.products AS n
UNION ALL 
SELECT DISTINCT 
      array('TestUser') AS `label`, 
      label AS `__order_col_0`
FROM test_integration.users AS n
) AS __union
ORDER BY __union.`__order_col_0` ASC
