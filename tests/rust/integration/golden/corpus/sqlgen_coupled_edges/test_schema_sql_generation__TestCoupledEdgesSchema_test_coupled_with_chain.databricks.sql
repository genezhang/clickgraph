WITH with_d_reqs_cte_0 AS (SELECT 
      any_value(t0.query) AS `p1_d_name`, 
      count(*) AS `reqs`
FROM zeek.dns_log AS t0
GROUP BY t0.query
HAVING reqs > 0
)
SELECT 
      d_reqs.p1_d_name AS `d.name`, 
      d_reqs.reqs AS `reqs`
FROM with_d_reqs_cte_0 AS d_reqs
