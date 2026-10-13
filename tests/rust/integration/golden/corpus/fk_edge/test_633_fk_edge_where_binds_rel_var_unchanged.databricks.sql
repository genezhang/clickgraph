SELECT 
      v1.customer_id AS `r.customer_id`
FROM test_integration.orders_fk AS v0
JOIN test_integration.orders_fk AS v1 ON v1.order_id = v0.order_id
JOIN test_integration.customers_fk AS v2 ON v1.customer_id = v2.customer_id
WHERE v1.customer_id > 2
ORDER BY v1.customer_id ASC NULLS LAST

-- column r.customer_id: Value [("r.customer_id", "r.customer_id")]