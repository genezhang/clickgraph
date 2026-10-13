SELECT 
      v2.name AS `c.name`, 
      v0.total_amount AS `o.total_amount`
FROM db_fk_edge.orders_fk AS v0
JOIN db_fk_edge.orders_fk AS v1 ON v1.order_id = v0.order_id
JOIN db_fk_edge.customers_fk AS v2 ON v1.customer_id = v2.customer_id
WHERE v0.total_amount > 100
ORDER BY v0.total_amount DESC NULLS FIRST
LIMIT 10
-- column c.name: Value [("c.name", "c.name")]
-- column o.total_amount: Value [("o.total_amount", "o.total_amount")]