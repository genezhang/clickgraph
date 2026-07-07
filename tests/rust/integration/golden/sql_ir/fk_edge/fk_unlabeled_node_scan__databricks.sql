SELECT `n.amount` AS `n.amount`, `n.customer_id` AS `n.customer_id`, `n.email` AS `n.email`, `n.name` AS `n.name`, `n.order_date` AS `n.order_date`, `n.order_id` AS `n.order_id` FROM (
SELECT 
      NULL AS `n.amount`, 
      string(n.customer_id) AS `n.customer_id`, 
      string(n.email) AS `n.email`, 
      string(n.name) AS `n.name`, 
      NULL AS `n.order_date`, 
      NULL AS `n.order_id`
FROM db_fk_edge.customers_fk AS n
UNION ALL 
SELECT 
      string(n.total_amount) AS `n.amount`, 
      NULL AS `n.customer_id`, 
      NULL AS `n.email`, 
      NULL AS `n.name`, 
      string(n.order_date) AS `n.order_date`, 
      string(n.order_id) AS `n.order_id`
FROM db_fk_edge.orders_fk AS n
) AS __union
LIMIT 25