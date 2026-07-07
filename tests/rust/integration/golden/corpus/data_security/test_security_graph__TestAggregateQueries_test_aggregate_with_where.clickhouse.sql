SELECT 
      g.name AS "g.name", 
      count(u.user_id) AS "count"
FROM data_security.ds_groups AS g
INNER JOIN data_security.ds_memberships AS t0 ON g.group_id = t0.group_id AND t0.member_type = 'User'
INNER JOIN data_security.ds_users AS u ON t0.member_id = u.user_id
WHERE startsWith(g.name, 'E')
GROUP BY g.name
