WITH subquery_s1 AS (
SELECT 
      v1.group_id AS "v0__group_id", 
      count(*) AS "__value"
FROM data_security.ds_memberships AS v1
JOIN data_security.ds_users AS v2 ON v1.member_id = v2.user_id
WHERE ((v1."member_type" = 'User'))
GROUP BY v1.group_id
), 
with_w2 AS (
SELECT 
      v0.group_id AS "v4__group_id", 
      v0.name AS "p2_v4_name", 
      coalesce(s1.__value, 0) AS "v5"
FROM data_security.ds_groups AS v0
LEFT JOIN subquery_s1 AS s1 ON v0.group_id = s1.v0__group_id
)
SELECT 
      w2.p2_v4_name AS "g.name", 
      w2.v5 AS "member_count"
FROM with_w2 AS w2

-- column g.name: Value [("g.name", "g.name")]
-- column member_count: Value [("member_count", "member_count")]