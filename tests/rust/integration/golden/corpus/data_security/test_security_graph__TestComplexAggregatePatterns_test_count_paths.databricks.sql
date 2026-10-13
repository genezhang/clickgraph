SELECT 
      count(1) AS `path_count`
FROM data_security.ds_users AS v0
JOIN data_security.ds_memberships AS v1 ON v1.member_id = v0.user_id
JOIN data_security.ds_groups AS v2 ON v1.group_id = v2.group_id
WHERE ((v1.`member_type` = 'User'))

-- column path_count: Value [("path_count", "path_count")]