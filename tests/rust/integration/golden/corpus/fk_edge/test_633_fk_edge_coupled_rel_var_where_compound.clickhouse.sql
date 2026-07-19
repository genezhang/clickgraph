SELECT 
      o.order_id AS "o.order_id"
FROM test_integration.orders_fk AS o
WHERE (o.amount > 0 AND o.customer_id > 2)
ORDER BY o.order_id ASC
