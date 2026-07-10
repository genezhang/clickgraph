SELECT `label` AS "label", count(*) AS "cnt" FROM (
SELECT 
      NULL AS "age",
      toString(n.category) AS "category",
      toString(n.name) AS "name",
      toString(n.price) AS "price",
      toString(n.product_id) AS "product_id",
      NULL AS "user_id",
      ['TestProduct', 'TestUser'] AS "label"
FROM test_integration.products AS n
UNION ALL 
SELECT 
      toString(n.age) AS "age",
      NULL AS "category",
      toString(n.name) AS "name",
      NULL AS "price",
      NULL AS "product_id",
      toString(n.user_id) AS "user_id",
      ['TestProduct', 'TestUser'] AS "label"
FROM test_integration.users AS n
) AS __union
GROUP BY `label`
ORDER BY `label` ASC
