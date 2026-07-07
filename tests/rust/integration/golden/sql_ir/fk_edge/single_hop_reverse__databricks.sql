SELECT 
      c.name AS `c.name`, 
      o.total_amount AS `o.amount`
FROM db_fk_edge.orders_fk AS o
INNER JOIN db_fk_edge.customers_fk AS c ON c.customer_id = o.customer_id
