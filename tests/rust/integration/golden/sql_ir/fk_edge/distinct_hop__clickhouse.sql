SELECT DISTINCT 
      c.name AS "c.name"
FROM db_fk_edge.orders_fk AS o
INNER JOIN db_fk_edge.customers_fk AS c ON c.customer_id = o.customer_id
