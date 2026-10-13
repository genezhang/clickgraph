WITH v1_rels AS (
SELECT 
      'RECEIVED' AS `__cg_type`, 
      'Actor' AS `__cg_from_label`, 
      'Actor' AS `__cg_to_label`, 
      e.evt_id AS `__cg_rid_0`, 
      e.dst_actor AS `__cg_from_0`, 
      e.src_actor AS `__cg_to_0`, 
      e.dst_actor AS `__cg_start_0`, 
      e.src_actor AS `__cg_end_0`, 
      'Actor' AS `__cg_start_label`, 
      'Actor' AS `__cg_end_label`, 
      concat('RECEIVED:Actor:Actor:', CAST(e.evt_id AS STRING)) AS `__cg_key`, 
      e.channel AS `p2_v1_channel`, 
      e.evt_id AS `p2_v1_evt_id__cg0`, 
      NULL AS `p2_v1_evt_id__cg1`, 
      NULL AS `p2_v1_weight`
FROM db_denorm_selfloop.events AS e

UNION ALL
SELECT 
      'SENT' AS `__cg_type`, 
      'Actor' AS `__cg_from_label`, 
      'Actor' AS `__cg_to_label`, 
      e.evt_id AS `__cg_rid_0`, 
      e.src_actor AS `__cg_from_0`, 
      e.dst_actor AS `__cg_to_0`, 
      e.src_actor AS `__cg_start_0`, 
      e.dst_actor AS `__cg_end_0`, 
      'Actor' AS `__cg_start_label`, 
      'Actor' AS `__cg_end_label`, 
      concat('SENT:Actor:Actor:', CAST(e.evt_id AS STRING)) AS `__cg_key`, 
      NULL AS `p2_v1_channel`, 
      NULL AS `p2_v1_evt_id__cg0`, 
      e.evt_id AS `p2_v1_evt_id__cg1`, 
      e.weight AS `p2_v1_weight`
FROM db_denorm_selfloop.events AS e
), 
__cg_nodes_Actor__cg_rows AS (
SELECT 
      e.src_actor AS `actor_id`, 
      e.src_name AS `name__cg0`, 
      NULL AS `name__cg1`
FROM db_denorm_selfloop.events AS e

UNION ALL
SELECT 
      e.dst_actor AS `actor_id`, 
      NULL AS `name__cg0`, 
      e.dst_name AS `name__cg1`
FROM db_denorm_selfloop.events AS e
), 
__cg_nodes_Actor AS (
SELECT 
      e.actor_id AS `actor_id`, 
      CASE WHEN count(DISTINCT CASE WHEN regexp_replace(regexp_replace(typeof(e.name__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.name__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.name__cg0, e.name__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END) > 1 THEN raise_error('ClickGraph: rows of one :Actor node hold several values of its property name: the property is not the node\'s, or its id columns do not identify it') ELSE any_value(CASE WHEN regexp_replace(regexp_replace(typeof(e.name__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.name__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.name__cg0, e.name__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, true) END AS `name`
FROM __cg_nodes_Actor__cg_rows AS e
WHERE NOT isnull(e.actor_id)
GROUP BY e.actor_id
)
SELECT 
      array(to_variant_object(map('elementId', CAST(concat('Actor:', CAST(v0.actor_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('Actor') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('actor_id', CAST(v0.actor_id AS VARIANT), 'name', CAST(v0.name AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))), to_variant_object(map('elementId', CAST(concat(v1.__cg_type, ':', CAST(v1.__cg_from_0 AS STRING), '->', CAST(v1.__cg_to_0 AS STRING), '-') AS VARIANT), 'startNodeElementId', CAST(concat(v1.__cg_from_label, ':', CAST(v1.__cg_from_0 AS STRING), '-') AS VARIANT), 'endNodeElementId', CAST(concat(v1.__cg_to_label, ':', CAST(v1.__cg_to_0 AS STRING), '-') AS VARIANT), 'type', CAST(v1.__cg_type AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('channel', CAST(v1.p2_v1_channel AS VARIANT), 'evt_id', CAST(CASE WHEN regexp_replace(regexp_replace(typeof(v1.p2_v1_evt_id__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v1.p2_v1_evt_id__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v1.p2_v1_evt_id__cg0, v1.p2_v1_evt_id__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AS VARIANT), 'weight', CAST(v1.p2_v1_weight AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))), to_variant_object(map('elementId', CAST(concat('Actor:', CAST(v2.actor_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('Actor') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('actor_id', CAST(v2.actor_id AS VARIANT), 'name', CAST(v2.name AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))) AS `p`
FROM __cg_nodes_Actor AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.actor_id
JOIN __cg_nodes_Actor AS v2 ON v1.__cg_end_0 = v2.actor_id
LIMIT 5
-- column p: Graph(Path) [("p", "p")]