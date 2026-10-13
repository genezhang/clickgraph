SELECT 
      v0.name AS "child", 
      v2.name AS "parent"
FROM data_security.ds_groups AS v0
JOIN data_security.ds_memberships AS v1 ON v1.member_id = v0.group_id
JOIN data_security.ds_groups AS v2 ON v1.group_id = v2.group_id
WHERE ((v1."member_type" = 'Group'))
ORDER BY v0.name ASC
