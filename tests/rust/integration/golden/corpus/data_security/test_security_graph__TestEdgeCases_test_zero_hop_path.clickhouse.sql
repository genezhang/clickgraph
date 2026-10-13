WITH RECURSIVE v2_labels AS (
SELECT 
      'Group' AS "__cg_label", 
      e.group_id AS "__cg_id_0", 
      NULL AS "p2_v2_department", 
      e.description AS "p2_v2_description", 
      NULL AS "p2_v2_email", 
      NULL AS "p2_v2_exposure", 
      e.group_id AS "p2_v2_group_id", 
      e.name AS "p2_v2_name__cg0", 
      NULL AS "p2_v2_name__cg1", 
      NULL AS "p2_v2_user_id"
FROM data_security.ds_groups AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      e.department AS "p2_v2_department", 
      NULL AS "p2_v2_description", 
      e.email AS "p2_v2_email", 
      e.exposure AS "p2_v2_exposure", 
      NULL AS "p2_v2_group_id", 
      NULL AS "p2_v2_name__cg0", 
      e.name AS "p2_v2_name__cg1", 
      e.user_id AS "p2_v2_user_id"
FROM data_security.ds_users AS e
), 
vlp_v1_nodes AS (
SELECT 
      'Group' AS "__cg_label", 
      e.group_id AS "__cg_id_0", 
      concat('Group', ':', toString(e.group_id)) AS "__cg_key"
FROM data_security.ds_groups AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      concat('User', ':', toString(e.user_id)) AS "__cg_key"
FROM data_security.ds_users AS e
), 
v1_rels AS (
SELECT 
      'MEMBER_OF' AS "__cg_type", 
      'Group' AS "__cg_from_label", 
      'Group' AS "__cg_to_label", 
      e.member_id AS "__cg_rid_0", 
      e.group_id AS "__cg_rid_1", 
      e.member_id AS "__cg_from_0", 
      e.group_id AS "__cg_to_0", 
      e.member_id AS "__cg_start_0", 
      e.group_id AS "__cg_end_0", 
      'Group' AS "__cg_start_label", 
      'Group' AS "__cg_end_label", 
      concat('MEMBER_OF:Group:Group:', toString(tuple(e.member_id, e.group_id))) AS "__cg_key"
FROM data_security.ds_memberships AS e
WHERE ((e."member_type" = 'Group'))

UNION ALL
SELECT 
      'MEMBER_OF' AS "__cg_type", 
      'User' AS "__cg_from_label", 
      'Group' AS "__cg_to_label", 
      e.member_id AS "__cg_rid_0", 
      e.group_id AS "__cg_rid_1", 
      e.member_id AS "__cg_from_0", 
      e.group_id AS "__cg_to_0", 
      e.member_id AS "__cg_start_0", 
      e.group_id AS "__cg_end_0", 
      'User' AS "__cg_start_label", 
      'Group' AS "__cg_end_label", 
      concat('MEMBER_OF:User:Group:', toString(tuple(e.member_id, e.group_id))) AS "__cg_key"
FROM data_security.ds_memberships AS e
WHERE ((e."member_type" = 'User'))
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS UInt32) AS hop_count, CAST([], 'Array(String)') AS path_edges, [start_node.__cg_key] AS path_nodes
    FROM vlp_v1_nodes AS start_node
    WHERE (start_node.__cg_label = 'User') AND ((start_node.__cg_label, start_node.__cg_id_0) IN (SELECT DISTINCT 
      'User' AS "label", 
      v0.user_id AS "id"
FROM data_security.ds_users AS v0
WHERE v0.name = 'Alice'))
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS UInt32) AS hop_count, arrayConcat(vp.path_edges, [rel.__cg_key]) AS path_edges, arrayConcat(vp.path_nodes, [end_node.__cg_key]) AS path_nodes
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 1
      AND NOT has(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v1_trails
)
SELECT 
      map('elementId', CAST(concat(v2.__cg_label, ':', toString(v2.__cg_id_0), '-'), 'Dynamic'), 'labels', CAST([v2.__cg_label], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('department', CAST(v2.p2_v2_department, 'Dynamic'), 'description', CAST(v2.p2_v2_description, 'Dynamic'), 'email', CAST(v2.p2_v2_email, 'Dynamic'), 'exposure', CAST(v2.p2_v2_exposure, 'Dynamic'), 'group_id', CAST(v2.p2_v2_group_id, 'Dynamic'), 'name', CAST(CAST(coalesce(v2.p2_v2_name__cg0, v2.p2_v2_name__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_name__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_name__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_name__cg0, v2.p2_v2_name__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_name__cg0, v2.p2_v2_name__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), 'Dynamic'), 'user_id', CAST(v2.p2_v2_user_id, 'Dynamic'))), 'Dynamic')) AS "target"
FROM vlp_v1_path AS v1
JOIN data_security.ds_users AS v0 ON v1.start_id = v0.user_id
JOIN v2_labels AS v2 ON v1.end_label = v2.__cg_label AND v1.end_id = v2.__cg_id_0
WHERE v0.name = 'Alice'
