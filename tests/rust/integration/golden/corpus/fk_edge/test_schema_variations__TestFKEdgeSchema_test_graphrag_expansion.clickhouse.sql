SELECT 
      0 AS "length(p)", 
      o.order_id AS "o.order_id"
FROM test_integration.orders_fk AS o
WHERE o.customer_id = 1
LIMIT 10