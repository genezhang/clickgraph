WITH with_w1 AS (
SELECT 
      v2.group_id AS "v3__group_id", 
      v2.name AS "p2_v3_name", 
      count(v0.user_id) AS "v4"
FROM data_security.ds_users AS v0
JOIN data_security.ds_memberships AS v1 ON v1.member_id = v0.user_id
JOIN data_security.ds_groups AS v2 ON v1.group_id = v2.group_id
WHERE ((v1."member_type" = 'User'))
GROUP BY v2.group_id, v2.name
HAVING count(v0.user_id) >= 1
)
SELECT 
      w1.p2_v3_name AS "g.name", 
      w1.v4 AS "user_count"
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST

-- column g.name: Value [("g.name", "g.name")]
-- column user_count: Value [("user_count", "user_count")]