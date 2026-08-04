WITH pattern_union_r AS (
(SELECT 'Order' AS start_type, string(test_integration.orders_fk.order_id) as start_id, string(test_integration.customers_fk.customer_id) as end_id, 'Customer' AS end_type, array('PLACED_BY') as path_relationships, array('{}') as rel_properties, to_json(struct(test_integration.orders_fk.order_date, test_integration.orders_fk.order_id, test_integration.orders_fk.total_amount)) as start_properties, to_json(struct(test_integration.customers_fk.customer_id, test_integration.customers_fk.email, test_integration.customers_fk.name)) as end_properties FROM test_integration.orders_fk INNER JOIN test_integration.customers_fk ON test_integration.customers_fk.customer_id = test_integration.orders_fk.customer_id)
UNION ALL
(SELECT 'Customer' AS start_type, string(test_integration.customers_fk.customer_id) as start_id, string(test_integration.orders_fk.order_id) as end_id, 'Order' AS end_type, array('PLACED_BY') as path_relationships, array('{}') as rel_properties, to_json(struct(test_integration.customers_fk.customer_id, test_integration.customers_fk.email, test_integration.customers_fk.name)) as start_properties, to_json(struct(test_integration.orders_fk.order_date, test_integration.orders_fk.order_id, test_integration.orders_fk.total_amount)) as end_properties FROM test_integration.orders_fk INNER JOIN test_integration.customers_fk ON test_integration.customers_fk.customer_id = test_integration.orders_fk.customer_id)
)
SELECT 
      count(*) AS `count(*)`
FROM pattern_union_r AS r
WHERE r.start_id = r.end_id AND r.start_type = r.end_type
