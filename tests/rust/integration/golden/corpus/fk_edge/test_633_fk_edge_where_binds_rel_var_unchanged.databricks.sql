SELECT 
      r.customer_id AS `r.customer_id`
FROM test_integration.orders_fk AS r
WHERE r.customer_id > 2
ORDER BY r.customer_id ASC
