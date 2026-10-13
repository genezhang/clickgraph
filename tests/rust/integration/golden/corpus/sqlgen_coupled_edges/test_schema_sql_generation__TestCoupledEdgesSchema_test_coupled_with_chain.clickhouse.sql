WITH with_w1 AS (
SELECT 
      v1.query AS "v3__name", 
      v1.query AS "p2_v3_name", 
      count(*) AS "v4"
FROM zeek.dns_log AS v1
WHERE (v1."id.orig_h" IS NOT NULL AND v1.query IS NOT NULL)
GROUP BY v1.query, v1.query
HAVING count(*) > 0
)
SELECT 
      w1.p2_v3_name AS "d.name", 
      w1.v4 AS "reqs"
FROM with_w1 AS w1

-- column d.name: Value [("d.name", "d.name")]
-- column reqs: Value [("reqs", "reqs")]