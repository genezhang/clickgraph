WITH with_cnt_prop_cte_0 AS (SELECT 
      r.answers AS `prop`, 
      count(*) AS `cnt`
FROM zeek.dns_log AS r
GROUP BY r.answers
)
SELECT 
      cnt_prop.prop AS `prop`, 
      cnt_prop.cnt AS `cnt`
FROM with_cnt_prop_cte_0 AS cnt_prop
ORDER BY cnt_prop.cnt DESC
LIMIT 10