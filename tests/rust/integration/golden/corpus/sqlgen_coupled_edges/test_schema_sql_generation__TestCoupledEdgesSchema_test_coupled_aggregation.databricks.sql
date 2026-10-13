SELECT 
      v1.query AS `d.name`, 
      count(*) AS `requests`
FROM zeek.dns_log AS v1
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL)
GROUP BY v1.query
ORDER BY count(*) DESC NULLS FIRST
LIMIT 10
-- column d.name: Value [("d.name", "d.name")]
-- column requests: Value [("requests", "requests")]