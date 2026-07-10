SELECT `label` AS `label`, count(*) AS `cnt` FROM (
SELECT 
      NULL AS `age`,
      string(n.category) AS `category`,
      string(n.name) AS `name`,
      string(n.price) AS `price`,
      string(n.product_id) AS `product_id`,
      NULL AS `user_id`,
      array('TestProduct', 'TestUser') AS `label`
FROM test_integration.products AS n
UNION ALL 
SELECT 
      string(n.age) AS `age`,
      NULL AS `category`,
      string(n.name) AS `name`,
      NULL AS `price`,
      NULL AS `product_id`,
      string(n.user_id) AS `user_id`,
      array('TestProduct', 'TestUser') AS `label`
FROM test_integration.users AS n
) AS __union
GROUP BY `label`
ORDER BY label ASC
