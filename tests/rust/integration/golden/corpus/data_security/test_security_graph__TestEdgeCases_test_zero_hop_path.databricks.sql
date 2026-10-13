WITH RECURSIVE v2_labels AS (
SELECT 
      'Group' AS `__cg_label`, 
      e.group_id AS `__cg_id_0`, 
      NULL AS `p2_v2_department`, 
      e.description AS `p2_v2_description`, 
      NULL AS `p2_v2_email`, 
      NULL AS `p2_v2_exposure`, 
      e.group_id AS `p2_v2_group_id`, 
      e.name AS `p2_v2_name__cg0`, 
      NULL AS `p2_v2_name__cg1`, 
      NULL AS `p2_v2_user_id`
FROM data_security.ds_groups AS e

UNION ALL
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      e.department AS `p2_v2_department`, 
      NULL AS `p2_v2_description`, 
      e.email AS `p2_v2_email`, 
      e.exposure AS `p2_v2_exposure`, 
      NULL AS `p2_v2_group_id`, 
      NULL AS `p2_v2_name__cg0`, 
      e.name AS `p2_v2_name__cg1`, 
      e.user_id AS `p2_v2_user_id`
FROM data_security.ds_users AS e
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
WHERE v0.name = 'Alice'))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 1
      AND NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v1_trails
)
SELECT 
      to_variant_object(map('elementId', CAST(concat(v2.__cg_label, ':', CAST(v2.__cg_id_0 AS STRING), '-') AS VARIANT), 'labels', CAST(array(v2.__cg_label) AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('department', CAST(v2.p2_v2_department AS VARIANT), 'description', CAST(v2.p2_v2_description AS VARIANT), 'email', CAST(v2.p2_v2_email AS VARIANT), 'exposure', CAST(v2.p2_v2_exposure AS VARIANT), 'group_id', CAST(v2.p2_v2_group_id AS VARIANT), 'name', CAST(CASE WHEN regexp_replace(regexp_replace(typeof(v2.p2_v2_name__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v2.p2_v2_name__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v2.p2_v2_name__cg0, v2.p2_v2_name__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS VARIANT), 'user_id', CAST(v2.p2_v2_user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))) AS `target`
FROM vlp_v1_path AS v1
JOIN data_security.ds_users AS v0 ON v1.start_id = v0.user_id
JOIN v2_labels AS v2 ON v1.end_label = v2.__cg_label AND v1.end_id = v2.__cg_id_0
WHERE v0.name = 'Alice'

-- column target: Graph(Node) [("target", "target")]