WITH optional_o1 AS (
SELECT 
      v1.customer_id AS `v0__customer_id`, 
      v2.order_id AS `v2__order_id`
FROM db_fk_edge.orders_fk AS v1
JOIN db_fk_edge.orders_fk AS v2 ON v1.order_id = v2.order_id
)
SELECT 
      v0.name AS `c.name`, 
      count(o1.v2__order_id) AS `orders`
FROM db_fk_edge.customers_fk AS v0
LEFT JOIN optional_o1 AS o1 ON v0.customer_id = o1.v0__customer_id
GROUP BY v0.name

-- column c.name: Value [("c.name", "c.name")]
-- column orders: Value [("orders", "orders")]