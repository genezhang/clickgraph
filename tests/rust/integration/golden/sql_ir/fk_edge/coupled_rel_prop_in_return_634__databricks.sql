SELECT 
      o.order_id AS `o.order_id`, 
      o.customer_id AS `r.customer_id`
FROM db_fk_edge.orders_fk AS o
