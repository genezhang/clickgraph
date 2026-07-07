SELECT 
      n.total_amount AS `n.amount`, 
      n.order_date AS `n.order_date`, 
      n.order_id AS `n.order_id`, 
      n.order_id AS `r.from_id`, 
      n.customer_id AS `r.to_id`, 
      o.customer_id AS `o.customer_id`, 
      o.email AS `o.email`, 
      o.name AS `o.name`
FROM db_fk_edge.orders_fk AS n
INNER JOIN db_fk_edge.customers_fk AS o ON o.customer_id = n.customer_id
LIMIT 25