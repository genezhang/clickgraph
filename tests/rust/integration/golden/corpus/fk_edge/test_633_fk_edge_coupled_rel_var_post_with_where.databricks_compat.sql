WITH with_w1 AS (
SELECT 
      v0.order_id AS `v3__order_id`, 
      v0.order_id AS `p2_v3_order_id`, 
      v1.order_id AS `v4__order_id`, 
      v1.customer_id AS `v4__customer_id`
FROM test_integration.orders_fk AS v0
JOIN test_integration.orders_fk AS v1 ON v1.order_id = v0.order_id
JOIN test_integration.customers_fk AS v2 ON v1.customer_id = v2.customer_id
WHERE NULL > 2
)
SELECT 
      w1.p2_v3_order_id AS `o.order_id`
FROM with_w1 AS w1
ORDER BY w1.p2_v3_order_id ASC NULLS LAST

-- column o.order_id: Value [("o.order_id", "o.order_id")]