WITH with_w1 AS (
SELECT 
      v0.order_id AS "v3__order_id", 
      v0.order_id AS "p2_v3_order_id", 
      count(v1.order_id) AS "v4"
FROM test_integration.orders_fk AS v0
JOIN test_integration.orders_fk AS v1 ON v1.order_id = v0.order_id
JOIN test_integration.customers_fk AS v2 ON v1.customer_id = v2.customer_id
WHERE v1.customer_id > 2
GROUP BY v0.order_id, v0.order_id
)
SELECT 
      w1.p2_v3_order_id AS "o.order_id", 
      w1.v4 AS "rc"
FROM with_w1 AS w1
ORDER BY w1.p2_v3_order_id ASC

-- column o.order_id: Value [("o.order_id", "o.order_id")]
-- column rc: Value [("rc", "rc")]