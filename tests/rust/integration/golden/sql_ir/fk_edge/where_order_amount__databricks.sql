SELECT 
      o.order_id AS `o.order_id`
FROM db_fk_edge.orders_fk AS o
WHERE o.total_amount > 100
