WITH v1_rels AS (
SELECT 
      'RECEIVED' AS "__cg_type", 
      'Actor' AS "__cg_from_label", 
      'Actor' AS "__cg_to_label", 
      e.evt_id AS "__cg_rid_0", 
      e.dst_actor AS "__cg_from_0", 
      e.src_actor AS "__cg_to_0", 
      e.dst_actor AS "__cg_start_0", 
      e.src_actor AS "__cg_end_0", 
      'Actor' AS "__cg_start_label", 
      'Actor' AS "__cg_end_label", 
      concat('RECEIVED:Actor:Actor:', toString(e.evt_id)) AS "__cg_key"
FROM db_denorm_selfloop.events AS e

UNION ALL
SELECT 
      'SENT' AS "__cg_type", 
      'Actor' AS "__cg_from_label", 
      'Actor' AS "__cg_to_label", 
      e.evt_id AS "__cg_rid_0", 
      e.src_actor AS "__cg_from_0", 
      e.dst_actor AS "__cg_to_0", 
      e.src_actor AS "__cg_start_0", 
      e.dst_actor AS "__cg_end_0", 
      'Actor' AS "__cg_start_label", 
      'Actor' AS "__cg_end_label", 
      concat('SENT:Actor:Actor:', toString(e.evt_id)) AS "__cg_key"
FROM db_denorm_selfloop.events AS e
), 
__cg_nodes_Actor__cg_rows AS (
SELECT 
      e.src_actor AS "actor_id", 
      e.src_name AS "name__cg0", 
      NULL AS "name__cg1"
FROM db_denorm_selfloop.events AS e

UNION ALL
SELECT 
      e.dst_actor AS "actor_id", 
      NULL AS "name__cg0", 
      e.dst_name AS "name__cg1"
FROM db_denorm_selfloop.events AS e
), 
__cg_nodes_Actor AS (
SELECT 
      e.actor_id AS "actor_id", 
      arrayElement([anyIfOrNull(CAST(coalesce(e.name__cg0, e.name__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.name__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.name__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.name__cg0, e.name__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.name__cg0, e.name__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), NOT isNull(CAST(coalesce(e.name__cg0, e.name__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.name__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.name__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.name__cg0, e.name__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.name__cg0, e.name__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types'))))], 1 + throwIf(uniqUpToIf(1)(cityHash64(CAST(coalesce(e.name__cg0, e.name__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.name__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.name__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.name__cg0, e.name__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.name__cg0, e.name__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types'))), NOT isNull(CAST(coalesce(e.name__cg0, e.name__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.name__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.name__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.name__cg0, e.name__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.name__cg0, e.name__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')))) > 1, 'ClickGraph: rows of one :Actor node hold several values of its property name: the property is not the node\'s, or its id columns do not identify it')) AS "name"
FROM __cg_nodes_Actor__cg_rows AS e
WHERE NOT isNull(e.actor_id)
GROUP BY e.actor_id
)
SELECT 
      count(*) AS "cnt"
FROM __cg_nodes_Actor AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.actor_id
JOIN __cg_nodes_Actor AS v2 ON v1.__cg_end_0 = v2.actor_id

-- column cnt: Value [("cnt", "cnt")]