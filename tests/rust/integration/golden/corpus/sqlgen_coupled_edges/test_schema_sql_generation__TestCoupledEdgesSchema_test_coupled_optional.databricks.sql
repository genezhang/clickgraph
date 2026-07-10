SELECT 
      t0.query AS `d.name`, 
      count(t0.answers) AS `resolutions`
FROM zeek.dns_log AS t0
GROUP BY t0.query
