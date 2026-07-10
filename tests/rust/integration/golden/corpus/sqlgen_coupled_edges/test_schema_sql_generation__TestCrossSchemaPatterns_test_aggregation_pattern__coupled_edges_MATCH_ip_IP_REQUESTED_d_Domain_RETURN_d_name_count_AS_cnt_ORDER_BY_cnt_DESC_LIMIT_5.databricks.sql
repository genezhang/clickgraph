SELECT 
      t0.query AS `d.name`, 
      count(*) AS `cnt`
FROM zeek.dns_log AS t0
GROUP BY t0.query
ORDER BY cnt DESC
LIMIT 5