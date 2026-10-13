WITH v0_labels AS (
SELECT 
      'Group' AS `__cg_label`, 
      e.group_id AS `__cg_id_0`
FROM data_security.ds_groups AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`
FROM data_security.ds_users AS e
), 
v1_rels AS (
SELECT 
      'MEMBER_OF' AS `__cg_type`, 
      'Group' AS `__cg_from_label`, 
      'Group' AS `__cg_to_label`, 
      e.member_id AS `__cg_rid_0`, 
      e.group_id AS `__cg_rid_1`, 
      e.member_id AS `__cg_from_0`, 
      e.group_id AS `__cg_to_0`, 
      e.member_id AS `__cg_start_0`, 
      e.group_id AS `__cg_end_0`, 
      'Group' AS `__cg_start_label`, 
      'Group' AS `__cg_end_label`, 
      concat('MEMBER_OF:Group:Group:', to_json(named_struct('_1', CAST(e.member_id AS STRING), '_2', CAST(e.group_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM data_security.ds_memberships AS e
WHERE ((e.`member_type` = 'Group'))

UNION ALL
SELECT 
      'MEMBER_OF' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'Group' AS `__cg_to_label`, 
      e.member_id AS `__cg_rid_0`, 
      e.group_id AS `__cg_rid_1`, 
      e.member_id AS `__cg_from_0`, 
      e.group_id AS `__cg_to_0`, 
      e.member_id AS `__cg_start_0`, 
      e.group_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'Group' AS `__cg_end_label`, 
      concat('MEMBER_OF:User:Group:', to_json(named_struct('_1', CAST(e.member_id AS STRING), '_2', CAST(e.group_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM data_security.ds_memberships AS e
WHERE ((e.`member_type` = 'User'))
)
SELECT DISTINCT 
      v2.name AS `g.name`
FROM v0_labels AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_label = v0.__cg_label AND v1.__cg_start_0 = v0.__cg_id_0
JOIN data_security.ds_groups AS v2 ON v1.__cg_end_0 = v2.group_id
ORDER BY `g.name` ASC NULLS LAST

-- column g.name: Value [("g.name", "g.name")]