SELECT count(`n.product_id`) AS `user_count` FROM (
SELECT 
      NULL AS `age`,
      string(n.category) AS `category`,
      string(n.name) AS `name`,
      string(n.price) AS `price`,
      string(n.product_id) AS `product_id`,
      NULL AS `user_id`,
      string(n.product_id) AS `n.product_id`
FROM test_integration.products AS n
WHERE labels(n) = array('TestUser')
UNION ALL 
SELECT 
      string(n.age) AS `age`,
      NULL AS `category`,
      string(n.name) AS `name`,
      NULL AS `price`,
      NULL AS `product_id`,
      string(n.user_id) AS `user_id`,
      NULL AS `n.product_id`
FROM test_integration.users AS n
WHERE labels(n) = array('TestUser')
) AS __union
