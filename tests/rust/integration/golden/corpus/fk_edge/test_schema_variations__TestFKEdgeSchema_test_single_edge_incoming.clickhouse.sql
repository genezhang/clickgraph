SELECT 
      o.order_id AS "o.order_id", 
      o.order_date AS "o.order_date"
FROM test_integration.orders_fk AS o
WHERE o.customer_id = 1
LIMIT 10