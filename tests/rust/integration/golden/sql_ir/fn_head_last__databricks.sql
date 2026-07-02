WITH with_ns_cte_0 AS (SELECT 
      collect_list(u.full_name) AS `ns`
FROM social.users_bench AS u
)
SELECT 
      element_at(ns.ns, 1) AS `h`, 
      element_at(ns.ns, -1) AS `l`
FROM with_ns_cte_0 AS ns
