SELECT 
      t0.`id.orig_h` AS `ip.ip`, 
      t0.query AS `d.name`, 
      t0.answers AS `rip.ip`
FROM zeek.dns_log AS t0
