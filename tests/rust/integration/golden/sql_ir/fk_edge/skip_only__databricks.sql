SELECT 
      o.order_id AS `o.order_id`
FROM db_fk_edge.orders_fk AS o
ORDER BY o.order_id ASC
OFFSET 2