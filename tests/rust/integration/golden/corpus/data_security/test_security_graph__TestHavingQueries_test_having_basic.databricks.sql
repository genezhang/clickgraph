WITH with_w1 AS (
SELECT 
      v0.name AS `v3`, 
      count(v2.user_id) AS `v4`
FROM data_security.ds_groups AS v0
JOIN data_security.ds_memberships AS v1 ON v1.group_id = v0.group_id
JOIN data_security.ds_users AS v2 ON v1.member_id = v2.user_id
WHERE ((v1.`member_type` = 'User'))
GROUP BY v0.name
HAVING count(v2.user_id) > 1
)
SELECT 
      w1.v3 AS `group_name`, 
      w1.v4 AS `member_count`
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST

-- column group_name: Value [("group_name", "group_name")]
-- column member_count: Value [("member_count", "member_count")]