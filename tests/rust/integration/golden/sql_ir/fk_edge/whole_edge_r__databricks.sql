SELECT 
      r.order_id AS `r.from_id`, 
      r.customer_id AS `r.to_id`
FROM db_fk_edge.orders_fk AS r
