WITH with_group_name_member_count_cte_0 AS (SELECT 
      g.name AS `group_name`, 
      count(u.user_id) AS `member_count`
FROM data_security.ds_users AS u
INNER JOIN data_security.ds_memberships AS t0 ON t0.member_id = u.user_id AND t0.member_type = 'User'
INNER JOIN data_security.ds_groups AS g ON g.group_id = t0.group_id
GROUP BY g.name
HAVING member_count > 1
)
SELECT 
      group_name_member_count.group_name AS `group_name`, 
      group_name_member_count.member_count AS `member_count`
FROM with_group_name_member_count_cte_0 AS group_name_member_count
ORDER BY group_name_member_count.member_count DESC
