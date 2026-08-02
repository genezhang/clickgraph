WITH with_ns_cte_0 AS (SELECT 
      groupArray(u.full_name) AS "ns"
FROM social.users_bench AS u
WHERE u.full_name = 'NOBODY'
)
SELECT 
      arrayElementOrNull(ns.ns, 1) AS "h", 
      arrayElementOrNull(ns.ns, -1) AS "l"
FROM with_ns_cte_0 AS ns
