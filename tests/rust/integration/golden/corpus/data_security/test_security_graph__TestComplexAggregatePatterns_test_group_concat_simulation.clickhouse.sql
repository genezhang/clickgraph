SELECT 
      v0.name AS "g.name", 
      coalesce(groupArray(v2.name), []) AS "member_names"
FROM data_security.ds_groups AS v0
JOIN data_security.ds_memberships AS v1 ON v1.group_id = v0.group_id
JOIN data_security.ds_users AS v2 ON v1.member_id = v2.user_id
WHERE ((v1."member_type" = 'User'))
GROUP BY v0.name
ORDER BY v0.name ASC

-- column g.name: Value [("g.name", "g.name")]
-- column member_names: Value [("member_names", "member_names")]