SELECT 
      t0.total_amount AS `t0.amount`, 
      t0.order_date AS `t0.order_date`, 
      t0.order_id AS `t0.order_id`, 
      t1.customer_id AS `t1.customer_id`, 
      t1.email AS `t1.email`, 
      t1.name AS `t1.name`, 
      struct('fixed_path', 't0', 't1', 't2') AS `p`
FROM db_fk_edge.orders_fk AS t0
INNER JOIN db_fk_edge.customers_fk AS t1 ON t1.customer_id = t0.customer_id
LIMIT 10