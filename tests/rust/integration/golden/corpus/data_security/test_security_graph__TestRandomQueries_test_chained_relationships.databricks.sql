SELECT 
      v0.name AS `u.name`, 
      v2.name AS `direct`, 
      v4.name AS `parent`
FROM data_security.ds_users AS v0
JOIN data_security.ds_memberships AS v1 ON v1.member_id = v0.user_id
JOIN data_security.ds_groups AS v2 ON v1.group_id = v2.group_id
JOIN data_security.ds_memberships AS v3 ON v3.member_id = v2.group_id
JOIN data_security.ds_groups AS v4 ON v3.group_id = v4.group_id
WHERE (((v1.`member_type` = 'User')) AND ((v3.`member_type` = 'Group')))
ORDER BY v0.name ASC NULLS LAST
