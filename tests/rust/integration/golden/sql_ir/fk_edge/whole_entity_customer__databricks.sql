SELECT 
      c.customer_id AS `c.customer_id`, 
      c.email AS `c.email`, 
      c.name AS `c.name`
FROM db_fk_edge.customers_fk AS c
