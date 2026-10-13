WITH with_w1 AS (
SELECT 
      v0.user_id AS `v3`, 
      count(v1.from_id) AS `v4`
FROM brahmand.users_bench AS v0
JOIN brahmand.interactions AS v1 ON v1.from_id = v0.user_id
JOIN brahmand.users_bench AS v2 ON v1.to_id = v2.user_id
WHERE ((((v1.`interaction_type` = 'FOLLOWS') AND (v1.`from_type` = 'User')) AND (v1.`to_type` = 'User')))
GROUP BY v0.user_id
)
SELECT 
      w1.v3 AS `prop`, 
      w1.v4 AS `cnt`
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST
LIMIT 10
-- column prop: Value [("prop", "prop")]
-- column cnt: Value [("cnt", "cnt")]