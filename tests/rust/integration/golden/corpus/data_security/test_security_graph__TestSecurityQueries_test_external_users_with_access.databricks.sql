WITH RECURSIVE v4_labels AS (
SELECT 
      'File' AS `__cg_label`, 
      e.fs_id AS `__cg_id_0`
FROM data_security.ds_fs_objects AS e
WHERE ((e.`fs_type` = 'File'))

UNION ALL
SELECT 
      'Folder' AS `__cg_label`, 
      e.fs_id AS `__cg_id_0`
FROM data_security.ds_fs_objects AS e
WHERE ((e.`fs_type` = 'Folder'))
), 
v3_rels AS (
SELECT 
      'HAS_ACCESS' AS `__cg_type`, 
      'Group' AS `__cg_from_label`, 
      'File' AS `__cg_to_label`, 
      e.subject_id AS `__cg_rid_0`, 
      e.object_id AS `__cg_rid_1`, 
      e.subject_id AS `__cg_from_0`, 
      e.object_id AS `__cg_to_0`, 
      e.subject_id AS `__cg_start_0`, 
      e.object_id AS `__cg_end_0`, 
      'Group' AS `__cg_start_label`, 
      'File' AS `__cg_end_label`, 
      concat('HAS_ACCESS:Group:File:', to_json(named_struct('_1', CAST(e.subject_id AS STRING), '_2', CAST(e.object_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM data_security.ds_permissions AS e
WHERE (((e.`subject_type` = 'Group') AND (e.`object_type` = 'File')))

UNION ALL
SELECT 
      'HAS_ACCESS' AS `__cg_type`, 
      'Group' AS `__cg_from_label`, 
      'Folder' AS `__cg_to_label`, 
      e.subject_id AS `__cg_rid_0`, 
      e.object_id AS `__cg_rid_1`, 
      e.subject_id AS `__cg_from_0`, 
      e.object_id AS `__cg_to_0`, 
      e.subject_id AS `__cg_start_0`, 
      e.object_id AS `__cg_end_0`, 
      'Group' AS `__cg_start_label`, 
      'Folder' AS `__cg_end_label`, 
      concat('HAS_ACCESS:Group:Folder:', to_json(named_struct('_1', CAST(e.subject_id AS STRING), '_2', CAST(e.object_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM data_security.ds_permissions AS e
WHERE (((e.`subject_type` = 'Group') AND (e.`object_type` = 'Folder')))
), 
vlp_v1_nodes AS (
SELECT 
      'Group' AS `__cg_label`, 
      e.group_id AS `__cg_id_0`, 
      concat('Group', ':', CAST(e.group_id AS STRING)) AS `__cg_key`
FROM data_security.ds_groups AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      concat('User', ':', CAST(e.user_id AS STRING)) AS `__cg_key`
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
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes
    FROM vlp_v1_nodes AS start_node
    WHERE (start_node.__cg_label = 'User') AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      'User' AS `label`, 
      v0.user_id AS `id`
FROM data_security.ds_users AS v0
WHERE v0.exposure = 'external'))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 5
      AND NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v1_trails
    WHERE hop_count >= 1
)
SELECT DISTINCT 
      v0.name AS `u.name`, 
      v2.name AS `via_group`
FROM vlp_v1_path AS v1
JOIN data_security.ds_users AS v0 ON v1.start_id = v0.user_id
JOIN data_security.ds_groups AS v2 ON v1.end_id = v2.group_id
JOIN v3_rels AS v3 ON v3.__cg_start_0 = v2.group_id
JOIN v4_labels AS v4 ON v3.__cg_end_label = v4.__cg_label AND v3.__cg_end_0 = v4.__cg_id_0
WHERE (v1.end_label = 'Group' AND v0.exposure = 'external')
ORDER BY `u.name` ASC NULLS LAST

-- column u.name: Value [("u.name", "u.name")]
-- column via_group: Value [("via_group", "via_group")]