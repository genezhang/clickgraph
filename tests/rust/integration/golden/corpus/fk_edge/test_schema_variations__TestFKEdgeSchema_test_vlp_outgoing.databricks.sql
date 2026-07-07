SELECT DISTINCT 
      o.order_id AS `o.order_id`, 
      o.total_amount AS `o.total_amount`
FROM test_integration.orders_fk AS o
WHERE o.customer_id = 1
LIMIT 10