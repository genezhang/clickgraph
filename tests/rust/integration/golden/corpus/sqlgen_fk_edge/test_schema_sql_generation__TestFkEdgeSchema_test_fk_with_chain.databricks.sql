WITH with_w1 AS (
SELECT 
      v0.customer_id AS `v3__customer_id`, 
      v0.name AS `p2_v3_name`, 
      count(v2.order_id) AS `v4`
FROM db_fk_edge.customers_fk AS v0
JOIN db_fk_edge.orders_fk AS v1 ON v1.customer_id = v0.customer_id
JOIN db_fk_edge.orders_fk AS v2 ON v1.order_id = v2.order_id
GROUP BY v0.customer_id, v0.name
HAVING count(v2.order_id) > 0
)
SELECT 
      w1.p2_v3_name AS `c.name`, 
      w1.v4 AS `orders`
FROM with_w1 AS w1

-- column c.name: Value [("c.name", "c.name")]
-- column orders: Value [("orders", "orders")]