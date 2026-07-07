SELECT 
      g.name AS `g.name`, 
      count(t0.member_id) AS `member_count`
FROM data_security.ds_groups AS g
LEFT JOIN (SELECT * FROM data_security.ds_memberships WHERE member_type = 'User') AS t0 ON t0.group_id = g.group_id
GROUP BY g.name
ORDER BY g.name ASC
