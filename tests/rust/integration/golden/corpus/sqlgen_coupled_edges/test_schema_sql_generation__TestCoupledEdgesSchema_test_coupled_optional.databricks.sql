SELECT 
      t0.query AS `d.name`, 
      count(rip.answers) AS `resolutions`
FROM zeek.dns_log AS t0
GROUP BY t0.query
