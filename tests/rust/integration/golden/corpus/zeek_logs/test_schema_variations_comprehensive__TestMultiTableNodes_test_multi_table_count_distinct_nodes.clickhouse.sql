SELECT 
      count(DISTINCT target."id.orig_h") AS "unique_ips"
FROM zeek.dns_log AS target
