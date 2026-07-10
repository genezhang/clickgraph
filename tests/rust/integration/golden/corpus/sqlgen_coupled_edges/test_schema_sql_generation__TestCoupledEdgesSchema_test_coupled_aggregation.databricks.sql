SELECT 
      t0.query AS `d.name`, 
      count(*) AS `requests`
FROM zeek.dns_log AS t0
GROUP BY t0.query
ORDER BY requests DESC
LIMIT 10