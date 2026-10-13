SELECT 
      v0.name AS `g.name`
FROM data_security.ds_groups AS v0
JOIN data_security.ds_memberships AS v1 ON v1.member_id = v0.group_id AND v1.group_id = v0.group_id
WHERE ((v1.`member_type` = 'Group'))
