SELECT 
      1 AS `length(p)`, 
      o.order_id AS `o.order_id`
FROM test_integration.customers_fk AS c
INNER JOIN test_integration.orders_fk AS o ON c.customer_id = o.customer_id
WHERE c.customer_id = 1
LIMIT 10