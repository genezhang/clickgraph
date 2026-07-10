WITH with_cnt_prop_cte_0 AS (SELECT 
      r.`id.orig_h` AS `prop`, 
      count(r.uid) AS `cnt`
FROM zeek.dns_log AS r
GROUP BY r.`id.orig_h`
)
SELECT 
      cnt_prop.prop AS `prop`, 
      cnt_prop.cnt AS `cnt`
FROM with_cnt_prop_cte_0 AS cnt_prop
ORDER BY cnt_prop.cnt DESC
LIMIT 10