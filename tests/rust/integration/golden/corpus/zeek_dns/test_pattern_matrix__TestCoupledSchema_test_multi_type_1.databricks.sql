SELECT 
      'REQUESTED' AS `type(r)`, 
      count(*) AS `cnt`
FROM zeek.dns_log AS r
GROUP BY 'REQUESTED'
