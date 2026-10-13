WITH v0_labels AS (
SELECT 
      'Group' AS "__cg_label", 
      e.group_id AS "__cg_id_0", 
      NULL AS "p2_v0_department", 
      e.description AS "p2_v0_description", 
      NULL AS "p2_v0_email", 
      NULL AS "p2_v0_exposure", 
      e.group_id AS "p2_v0_group_id", 
      e.name AS "p2_v0_name__cg0", 
      NULL AS "p2_v0_name__cg1", 
      NULL AS "p2_v0_user_id"
FROM data_security.ds_groups AS e

UNION ALL
SELECT 
      'User' AS "__cg_label", 
      e.user_id AS "__cg_id_0", 
      e.department AS "p2_v0_department", 
      NULL AS "p2_v0_description", 
      e.email AS "p2_v0_email", 
      e.exposure AS "p2_v0_exposure", 
      NULL AS "p2_v0_group_id", 
      NULL AS "p2_v0_name__cg0", 
      e.name AS "p2_v0_name__cg1", 
      e.user_id AS "p2_v0_user_id"
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
)
SELECT 
      map('elementId', CAST(concat(v0.__cg_label, ':', toString(v0.__cg_id_0), '-'), 'Dynamic'), 'labels', CAST([v0.__cg_label], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('department', CAST(v0.p2_v0_department, 'Dynamic'), 'description', CAST(v0.p2_v0_description, 'Dynamic'), 'email', CAST(v0.p2_v0_email, 'Dynamic'), 'exposure', CAST(v0.p2_v0_exposure, 'Dynamic'), 'group_id', CAST(v0.p2_v0_group_id, 'Dynamic'), 'name', CAST(CAST(coalesce(v0.p2_v0_name__cg0, v0.p2_v0_name__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_name__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v0.p2_v0_name__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v0.p2_v0_name__cg0, v0.p2_v0_name__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v0.p2_v0_name__cg0, v0.p2_v0_name__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), 'Dynamic'), 'user_id', CAST(v0.p2_v0_user_id, 'Dynamic'))), 'Dynamic')) AS "member"
FROM v0_labels AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_label = v0.__cg_label AND v1.__cg_start_0 = v0.__cg_id_0
JOIN data_security.ds_groups AS v2 ON v1.__cg_end_0 = v2.group_id
WHERE v2.name = 'Engineering'
