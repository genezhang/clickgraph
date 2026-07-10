WITH pattern_union_r AS (
(SELECT 'Order' AS start_type, string(db_fk_edge.orders_fk.order_id) as start_id, string(db_fk_edge.customers_fk.customer_id) as end_id, 'Customer' AS end_type, array('PLACED_BY') as path_relationships, array('{}') as rel_properties, to_json(struct(db_fk_edge.orders_fk.total_amount, db_fk_edge.orders_fk.order_date, db_fk_edge.orders_fk.order_id)) as start_properties, to_json(struct(db_fk_edge.customers_fk.customer_id, db_fk_edge.customers_fk.email, db_fk_edge.customers_fk.name)) as end_properties FROM db_fk_edge.orders_fk INNER JOIN db_fk_edge.customers_fk ON db_fk_edge.customers_fk.customer_id = db_fk_edge.orders_fk.customer_id)
UNION ALL
(SELECT 'Customer' AS start_type, string(db_fk_edge.customers_fk.customer_id) as start_id, string(db_fk_edge.orders_fk.order_id) as end_id, 'Order' AS end_type, array('PLACED_BY') as path_relationships, array('{}') as rel_properties, to_json(struct(db_fk_edge.customers_fk.customer_id, db_fk_edge.customers_fk.email, db_fk_edge.customers_fk.name)) as start_properties, to_json(struct(db_fk_edge.orders_fk.total_amount, db_fk_edge.orders_fk.order_date, db_fk_edge.orders_fk.order_id)) as end_properties FROM db_fk_edge.orders_fk INNER JOIN db_fk_edge.customers_fk ON db_fk_edge.customers_fk.customer_id = db_fk_edge.orders_fk.customer_id)
)
SELECT 
      r.start_properties AS `n.properties`, 
      r.start_id AS `n.id`, 
      r.start_type AS `n.__label__`, 
      r.path_relationships AS `r.type`, 
      r.rel_properties AS `r.properties`, 
      r.start_id AS `r.start_id`, 
      r.end_id AS `r.end_id`, 
      r.end_properties AS `o.properties`, 
      r.end_id AS `o.id`, 
      r.end_type AS `o.__label__`
FROM pattern_union_r AS r
LIMIT 25