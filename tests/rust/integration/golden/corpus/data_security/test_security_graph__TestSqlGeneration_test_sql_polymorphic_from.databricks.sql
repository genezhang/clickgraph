WITH v0_labels AS (
SELECT 
      'Group' AS `__cg_label`, 
      e.group_id AS `__cg_id_0`, 
      NULL AS `p2_v0_department`, 
      e.description AS `p2_v0_description`, 
      NULL AS `p2_v0_email`, 
      NULL AS `p2_v0_exposure`, 
      e.group_id AS `p2_v0_group_id`, 
      e.name AS `p2_v0_name__cg0`, 
      NULL AS `p2_v0_name__cg1`, 
      NULL AS `p2_v0_user_id`
FROM data_security.ds_groups AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      e.department AS `p2_v0_department`, 
      NULL AS `p2_v0_description`, 
      e.email AS `p2_v0_email`, 
      e.exposure AS `p2_v0_exposure`, 
      NULL AS `p2_v0_group_id`, 
      NULL AS `p2_v0_name__cg0`, 
      e.name AS `p2_v0_name__cg1`, 
      e.user_id AS `p2_v0_user_id`
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
SELECT 
      to_variant_object(map('elementId', CAST(concat(v0.__cg_label, ':', CAST(v0.__cg_id_0 AS STRING), '-') AS VARIANT), 'labels', CAST(array(v0.__cg_label) AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('department', CAST(v0.p2_v0_department AS VARIANT), 'description', CAST(v0.p2_v0_description AS VARIANT), 'email', CAST(v0.p2_v0_email AS VARIANT), 'exposure', CAST(v0.p2_v0_exposure AS VARIANT), 'group_id', CAST(v0.p2_v0_group_id AS VARIANT), 'name', CAST(CASE WHEN regexp_replace(regexp_replace(typeof(v0.p2_v0_name__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v0.p2_v0_name__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v0.p2_v0_name__cg0, v0.p2_v0_name__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS VARIANT), 'user_id', CAST(v0.p2_v0_user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))) AS `member`
FROM v0_labels AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_label = v0.__cg_label AND v1.__cg_start_0 = v0.__cg_id_0
JOIN data_security.ds_groups AS v2 ON v1.__cg_end_0 = v2.group_id
WHERE v2.name = 'Engineering'
