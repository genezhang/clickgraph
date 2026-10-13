WITH v0_labels AS (
SELECT 
      'TestProduct' AS `__cg_label`, 
      e.product_id AS `__cg_id_0`
FROM test_integration.products AS e

UNION ALL
SELECT 
      'TestUser' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`
FROM test_integration.users AS e
)
SELECT 
      count(v0.__cg_label) AS `user_count`
FROM v0_labels AS v0
WHERE array(v0.__cg_label) = array('TestUser')
