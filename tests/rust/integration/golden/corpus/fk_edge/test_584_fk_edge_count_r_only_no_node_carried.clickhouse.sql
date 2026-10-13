WITH with_w1 AS (
SELECT 
      count(v1.order_id) AS "v3"
FROM test_integration.orders_fk AS v0
JOIN test_integration.orders_fk AS v1 ON v1.order_id = v0.order_id
JOIN test_integration.customers_fk AS v2 ON v1.customer_id = v2.customer_id
)
SELECT 
      w1.v3 AS "rc"
FROM with_w1 AS w1

-- column rc: Value [("rc", "rc")]