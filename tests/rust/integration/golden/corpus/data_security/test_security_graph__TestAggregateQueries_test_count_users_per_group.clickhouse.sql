SELECT 
      v0.name AS "group_name", 
      count(v2.user_id) AS "member_count"
FROM data_security.ds_groups AS v0
JOIN data_security.ds_memberships AS v1 ON v1.group_id = v0.group_id
JOIN data_security.ds_users AS v2 ON v1.member_id = v2.user_id
WHERE ((v1."member_type" = 'User'))
GROUP BY v0.name
ORDER BY count(v2.user_id) DESC NULLS FIRST

-- column group_name: Value [("group_name", "group_name")]
-- column member_count: Value [("member_count", "member_count")]