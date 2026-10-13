WITH v2_labels AS (
SELECT 
      'File' AS "__cg_label", 
      e.fs_id AS "__cg_id_0", 
      e.fs_id AS "p2_v2_fs_id__cg0", 
      NULL AS "p2_v2_fs_id__cg1", 
      e.name AS "p2_v2_name__cg0", 
      NULL AS "p2_v2_name__cg1", 
      e.parent_id AS "p2_v2_parent_id__cg0", 
      NULL AS "p2_v2_parent_id__cg1", 
      e.path AS "p2_v2_path__cg0", 
      NULL AS "p2_v2_path__cg1", 
      e.sensitive_data AS "p2_v2_sensitive_data"
FROM data_security.ds_fs_objects AS e
WHERE ((e."fs_type" = 'File'))

UNION ALL
SELECT 
      'Folder' AS "__cg_label", 
      e.fs_id AS "__cg_id_0", 
      NULL AS "p2_v2_fs_id__cg0", 
      e.fs_id AS "p2_v2_fs_id__cg1", 
      NULL AS "p2_v2_name__cg0", 
      e.name AS "p2_v2_name__cg1", 
      NULL AS "p2_v2_parent_id__cg0", 
      e.parent_id AS "p2_v2_parent_id__cg1", 
      NULL AS "p2_v2_path__cg0", 
      e.path AS "p2_v2_path__cg1", 
      NULL AS "p2_v2_sensitive_data"
FROM data_security.ds_fs_objects AS e
WHERE ((e."fs_type" = 'Folder'))
), 
v1_rels AS (
SELECT 
      'CONTAINS' AS "__cg_type", 
      'Folder' AS "__cg_from_label", 
      'File' AS "__cg_to_label", 
      e.parent_id AS "__cg_rid_0", 
      e.fs_id AS "__cg_rid_1", 
      e.parent_id AS "__cg_from_0", 
      e.fs_id AS "__cg_to_0", 
      e.parent_id AS "__cg_start_0", 
      e.fs_id AS "__cg_end_0", 
      'Folder' AS "__cg_start_label", 
      'File' AS "__cg_end_label", 
      concat('CONTAINS:Folder:File:', toString(tuple(e.parent_id, e.fs_id))) AS "__cg_key"
FROM data_security.ds_fs_objects AS e
WHERE ((e."fs_type" = 'File'))

UNION ALL
SELECT 
      'CONTAINS' AS "__cg_type", 
      'Folder' AS "__cg_from_label", 
      'Folder' AS "__cg_to_label", 
      e.parent_id AS "__cg_rid_0", 
      e.fs_id AS "__cg_rid_1", 
      e.parent_id AS "__cg_from_0", 
      e.fs_id AS "__cg_to_0", 
      e.parent_id AS "__cg_start_0", 
      e.fs_id AS "__cg_end_0", 
      'Folder' AS "__cg_start_label", 
      'Folder' AS "__cg_end_label", 
      concat('CONTAINS:Folder:Folder:', toString(tuple(e.parent_id, e.fs_id))) AS "__cg_key"
FROM data_security.ds_fs_objects AS e
WHERE ((e."fs_type" = 'Folder'))
)
SELECT 
      map('elementId', CAST(concat(v2.__cg_label, ':', toString(v2.__cg_id_0), '-'), 'Dynamic'), 'labels', CAST([v2.__cg_label], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('fs_id', CAST(CAST(coalesce(v2.p2_v2_fs_id__cg0, v2.p2_v2_fs_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_fs_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_fs_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_fs_id__cg0, v2.p2_v2_fs_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_fs_id__cg0, v2.p2_v2_fs_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), 'Dynamic'), 'name', CAST(CAST(coalesce(v2.p2_v2_name__cg0, v2.p2_v2_name__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_name__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_name__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_name__cg0, v2.p2_v2_name__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_name__cg0, v2.p2_v2_name__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), 'Dynamic'), 'parent_id', CAST(CAST(coalesce(v2.p2_v2_parent_id__cg0, v2.p2_v2_parent_id__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_parent_id__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_parent_id__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_parent_id__cg0, v2.p2_v2_parent_id__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_parent_id__cg0, v2.p2_v2_parent_id__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), 'Dynamic'), 'path', CAST(CAST(coalesce(v2.p2_v2_path__cg0, v2.p2_v2_path__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_path__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(v2.p2_v2_path__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(v2.p2_v2_path__cg0, v2.p2_v2_path__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(v2.p2_v2_path__cg0, v2.p2_v2_path__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), 'Dynamic'), 'sensitive_data', CAST(v2.p2_v2_sensitive_data, 'Dynamic'))), 'Dynamic')) AS "child"
FROM data_security.ds_fs_objects AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.fs_id
JOIN v2_labels AS v2 ON v1.__cg_end_label = v2.__cg_label AND v1.__cg_end_0 = v2.__cg_id_0
WHERE (((v0."fs_type" = 'Folder')) AND v0.name = 'docs')

-- column child: Graph(Node) [("child", "child")]