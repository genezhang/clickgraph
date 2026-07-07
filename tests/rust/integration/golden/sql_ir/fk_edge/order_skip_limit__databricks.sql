SELECT 
      o.order_id AS `o.order_id`, 
      o.total_amount AS `o.amount`
FROM db_fk_edge.orders_fk AS o
ORDER BY o.total_amount DESC
LIMIT 3 OFFSET 1