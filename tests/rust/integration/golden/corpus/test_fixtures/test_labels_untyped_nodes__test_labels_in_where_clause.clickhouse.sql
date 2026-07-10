SELECT count(coalesce(`n.product_id`, `n.user_id`)) AS "user_count" FROM (
SELECT 
      NULL AS "age",
      toString(n.category) AS "category",
      toString(n.name) AS "name",
      toString(n.price) AS "price",
      toString(n.product_id) AS "product_id",
      NULL AS "user_id",
      toString(n.product_id) AS "n.product_id",
      NULL AS "n.user_id"
FROM test_integration.products AS n
WHERE labels(n) = ['TestUser']
UNION ALL 
SELECT 
      toString(n.age) AS "age",
      NULL AS "category",
      toString(n.name) AS "name",
      NULL AS "price",
      NULL AS "product_id",
      toString(n.user_id) AS "user_id",
      NULL AS "n.product_id",
      toString(n.user_id) AS "n.user_id"
FROM test_integration.users AS n
WHERE labels(n) = ['TestUser']
) AS __union
