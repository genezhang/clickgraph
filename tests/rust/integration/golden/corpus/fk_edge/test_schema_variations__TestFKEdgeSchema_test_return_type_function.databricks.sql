SELECT 
      'PLACED_BY' AS `type(r)`, 
      c.name AS `c.name`
FROM test_integration.orders_fk AS o
INNER JOIN test_integration.customers_fk AS c ON c.customer_id = o.customer_id
WHERE o.order_id = 1
LIMIT 5