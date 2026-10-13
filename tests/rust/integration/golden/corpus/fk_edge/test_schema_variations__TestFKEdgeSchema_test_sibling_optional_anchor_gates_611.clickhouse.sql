WITH optional_o1 AS (
SELECT 
      v0.order_id AS "v0__order_id", 
      v2.customer_id AS "v2__customer_id", 
      v2.customer_id AS "p2_v2_customer_id"
FROM test_integration.orders_fk AS v0
JOIN test_integration.orders_fk AS v1 ON v1.order_id = v0.order_id
JOIN test_integration.customers_fk AS v2 ON v1.customer_id = v2.customer_id
WHERE v0.total_amount > 100
), 
optional_o2 AS (
SELECT 
      v0.order_id AS "v0__order_id", 
      v4.customer_id AS "v4__customer_id", 
      v4.customer_id AS "p2_v4_customer_id"
FROM test_integration.orders_fk AS v0
JOIN test_integration.orders_fk AS v3 ON v3.order_id = v0.order_id
JOIN test_integration.customers_fk AS v4 ON v3.customer_id = v4.customer_id
WHERE v0.total_amount > 200
)
SELECT 
      v0.order_id AS "o.order_id", 
      o1.p2_v2_customer_id AS "c.customer_id", 
      o2.p2_v4_customer_id AS "d.customer_id"
FROM test_integration.orders_fk AS v0
LEFT JOIN optional_o1 AS o1 ON v0.order_id = o1.v0__order_id
LEFT JOIN optional_o2 AS o2 ON v0.order_id = o2.v0__order_id
ORDER BY v0.order_id ASC

-- column o.order_id: Value [("o.order_id", "o.order_id")]
-- column c.customer_id: Value [("c.customer_id", "c.customer_id")]
-- column d.customer_id: Value [("d.customer_id", "d.customer_id")]