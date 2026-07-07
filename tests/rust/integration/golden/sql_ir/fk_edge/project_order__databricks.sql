SELECT 
      o.order_id AS `o.order_id`, 
      o.order_date AS `o.order_date`, 
      o.total_amount AS `o.amount`
FROM db_fk_edge.orders_fk AS o
