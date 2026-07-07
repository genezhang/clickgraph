SELECT 
      c.name AS "c.name"
FROM db_composite_id.customers AS c
WHERE c.city IN ['New York', 'Chicago']
