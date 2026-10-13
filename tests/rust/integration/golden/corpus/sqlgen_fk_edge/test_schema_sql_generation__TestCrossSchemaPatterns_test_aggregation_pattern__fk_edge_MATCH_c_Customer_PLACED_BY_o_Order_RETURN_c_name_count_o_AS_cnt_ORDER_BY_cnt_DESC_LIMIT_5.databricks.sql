SELECT 
      v0.name AS `c.name`, 
      count(v2.order_id) AS `cnt`
FROM db_fk_edge.customers_fk AS v0
JOIN db_fk_edge.orders_fk AS v1 ON v1.customer_id = v0.customer_id
JOIN db_fk_edge.orders_fk AS v2 ON v1.order_id = v2.order_id
GROUP BY v0.name
ORDER BY count(v2.order_id) DESC NULLS FIRST
LIMIT 5
-- column c.name: Value [("c.name", "c.name")]
-- column cnt: Value [("cnt", "cnt")]