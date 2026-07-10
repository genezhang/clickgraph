SELECT 
      req.`id.orig_h` AS `src.ip`, 
      req.query AS `d.name`, 
      req.answers AS `rip.ip`, 
      req.ts AS `req.timestamp`
FROM zeek.dns_log AS req
WHERE req.query = 'example.com'
