SELECT 
      v0.name AS "c.name", 
      count(v2.order_id) AS "orders", 
      coalesce(sum(v2.total_amount), 0) AS "total"
FROM db_fk_edge.customers_fk AS v0
JOIN db_fk_edge.orders_fk AS v1 ON v1.customer_id = v0.customer_id
JOIN db_fk_edge.orders_fk AS v2 ON v1.order_id = v2.order_id
GROUP BY v0.name

-- column c.name: Value [("c.name", "c.name")]
-- column orders: Value [("orders", "orders")]
-- column total: Value [("total", "total")]