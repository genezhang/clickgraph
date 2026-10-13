SELECT 
      count(DISTINCT v0.`id.orig_h`) AS `unique_ips`
FROM zeek.dns_log AS v0
JOIN zeek.dns_log AS v1 ON v1.`id.orig_h` = v0.`id.orig_h`
JOIN zeek.dns_log AS v2 ON v1.query = v2.query

-- column unique_ips: Value [("unique_ips", "unique_ips")]