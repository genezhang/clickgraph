SELECT 
      o.order_id AS "o.order_id", 
      c.customer_id AS "c.customer_id", 
      d.customer_id AS "d.customer_id"
FROM test_integration.orders_fk AS o
LEFT JOIN test_integration.customers_fk AS c ON c.customer_id = o.customer_id AND o.total_amount > 100
LEFT JOIN test_integration.customers_fk AS d ON d.customer_id = o.customer_id AND o.total_amount > 200
ORDER BY o.order_id ASC
