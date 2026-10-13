WITH optional_o1 AS (
SELECT 
      v3.query AS `v2__name`, 
      v3.answers AS `v4__ip`
FROM zeek.dns_log AS v3
WHERE v3.answers IS NOT NULL
)
SELECT 
      v1.query AS `d.name`, 
      count(o1.v4__ip) AS `resolutions`
FROM zeek.dns_log AS v1
LEFT JOIN optional_o1 AS o1 ON v1.query = o1.v2__name
WHERE (v1.`id.orig_h` IS NOT NULL AND v1.query IS NOT NULL)
GROUP BY v1.query
