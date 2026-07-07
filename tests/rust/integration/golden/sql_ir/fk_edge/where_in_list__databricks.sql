SELECT 
      c.email AS `c.email`
FROM db_fk_edge.customers_fk AS c
WHERE c.name IN ('Alice', 'Bob')
