SELECT `_rel_properties` AS `_rel_properties`, `__rel_type__` AS `__rel_type__`, `__start_label__` AS `__start_label__`, `__end_label__` AS `__end_label__`, `b.name` AS `b.name`, `path_length` AS `path_length` FROM (
SELECT 
      '{}' AS "_rel_properties", 
      'TEST_FOLLOWS' AS "__rel_type__", 
      'TestUser' AS "__start_label__", 
      'TestUser' AS "__end_label__", 
      b.name AS "b.name", 
      1 AS "path_length", 
      b.name AS "__order_col_0"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON t0.follower_id = a.user_id
INNER JOIN test_integration.users AS b ON b.user_id = t0.followed_id
WHERE a.name = 'Alice'
UNION ALL 
SELECT 
      '{}' AS "_rel_properties", 
      'TEST_FOLLOWS' AS "__rel_type__", 
      'TestUser' AS "__start_label__", 
      'TestUser' AS "__end_label__", 
      b.name AS "b.name", 
      length(p) AS "path_length", 
      b.name AS "__order_col_0"
FROM test_integration.users AS a
INNER JOIN test_integration.follows AS t0 ON a.user_id = t0.followed_id
INNER JOIN test_integration.users AS b ON t0.follower_id = b.user_id
WHERE a.name = 'Alice'
) AS __union
ORDER BY __union.`__order_col_0` ASC
