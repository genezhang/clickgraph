SELECT 
      'PLACED_BY' AS "type(r)", 
      v2.name AS "c.name"
FROM test_integration.orders_fk AS v0
JOIN test_integration.orders_fk AS v1 ON v1.order_id = v0.order_id
JOIN test_integration.customers_fk AS v2 ON v1.customer_id = v2.customer_id
WHERE v0.order_id = 1
LIMIT 5
-- column type(r): Value [("type(r)", "type(r)")]
-- column c.name: Value [("c.name", "c.name")]