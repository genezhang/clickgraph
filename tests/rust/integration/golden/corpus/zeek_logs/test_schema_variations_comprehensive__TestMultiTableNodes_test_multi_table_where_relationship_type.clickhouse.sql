SELECT 
      count(*) AS "total"
FROM zeek.dns_log AS r
WHERE type(r) = 'REQUESTED'
