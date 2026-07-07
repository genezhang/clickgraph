SELECT 
      c.customer_id AS `c.customer_id`, 
      c.name AS `c.name`, 
      c.email AS `c.email`
FROM db_fk_edge.customers_fk AS c
