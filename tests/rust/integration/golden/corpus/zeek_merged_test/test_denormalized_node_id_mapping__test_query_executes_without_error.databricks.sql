SELECT 
      t0.`id.orig_h` AS `src.ip`, 
      t0.query AS `d.name`
FROM zeek.dns_log AS t0
WHERE t0.query = 'cdn.example.com'
