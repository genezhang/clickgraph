SELECT 
      o.order_id AS `o.order_id`
FROM db_fk_edge.orders_fk AS o
INNER JOIN db_fk_edge.customers_fk AS c ON c.customer_id = o.customer_id
WHERE (o.total_amount > 100 AND c.name = 'Alice')
