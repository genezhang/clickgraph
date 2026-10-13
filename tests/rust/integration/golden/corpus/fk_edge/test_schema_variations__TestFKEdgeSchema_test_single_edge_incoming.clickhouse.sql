SELECT 
      v2.order_id AS "o.order_id", 
      v2.order_date AS "o.order_date"
FROM test_integration.customers_fk AS v0
JOIN test_integration.orders_fk AS v1 ON v1.customer_id = v0.customer_id
JOIN test_integration.orders_fk AS v2 ON v1.order_id = v2.order_id
WHERE v0.customer_id = 1
LIMIT 10
-- column o.order_id: Value [("o.order_id", "o.order_id")]
-- column o.order_date: Value [("o.order_date", "o.order_date")]