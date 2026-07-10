SELECT 
      g.name AS "g.name", 
      count(DISTINCT t0.object_id) AS "accessible_files"
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_memberships AS t1 ON t1.member_id = u.user_id AND t1.member_type = 'User'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t1.group_id
INNER JOIN data_security.ds_permissions AS t0 ON t0.subject_id = u.user_id AND t0.subject_type = 'User' AND t0.object_type = 'File'
GROUP BY g.name
ORDER BY accessible_files DESC
