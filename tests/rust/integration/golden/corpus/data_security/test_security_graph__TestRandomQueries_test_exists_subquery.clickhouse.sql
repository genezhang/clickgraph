WITH subquery_s1 AS (
SELECT 
      v1.member_id AS "v0__user_id", 
      CAST(count(*) > 0 AS Nullable(Bool)) AS "__value"
FROM data_security.ds_memberships AS v1
JOIN data_security.ds_groups AS v2 ON v1.group_id = v2.group_id
WHERE (((v1."member_type" = 'User')) AND v2.name = 'Engineering')
GROUP BY v1.member_id
)
SELECT 
      v0.name AS "u.name"
FROM data_security.ds_users AS v0
LEFT JOIN subquery_s1 AS s1 ON v0.user_id = s1.v0__user_id
WHERE coalesce(s1.__value, false)

-- column u.name: Value [("u.name", "u.name")]