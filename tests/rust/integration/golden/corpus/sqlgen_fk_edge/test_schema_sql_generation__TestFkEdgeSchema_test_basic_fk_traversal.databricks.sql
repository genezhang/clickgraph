SELECT 
      v0.order_id AS `o.order_id`, 
      v2.name AS `c.name`
FROM db_fk_edge.orders_fk AS v0
JOIN db_fk_edge.orders_fk AS v1 ON v1.order_id = v0.order_id
JOIN db_fk_edge.customers_fk AS v2 ON v1.customer_id = v2.customer_id
