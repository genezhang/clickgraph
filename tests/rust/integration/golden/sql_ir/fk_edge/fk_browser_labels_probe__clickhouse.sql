SELECT DISTINCT 
      ['Customer'] AS "labels(n)"
FROM db_fk_edge.customers_fk AS n
UNION ALL 
SELECT DISTINCT 
      ['Order'] AS "labels(n)"
FROM db_fk_edge.orders_fk AS n
