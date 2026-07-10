SELECT 
      g.name AS "g.name", 
      count(u.user_id) AS "member_count"
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = u.user_id AND t0.member_type = 'User'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
GROUP BY g.name
ORDER BY member_count DESC
