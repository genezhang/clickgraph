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
      concat('RECEIVED:Actor:Actor:', CAST(e.evt_id AS STRING)) AS `__cg_key`
FROM db_denorm_selfloop.events AS e

UNION ALL
SELECT 
      'RECEIVED' AS `__cg_type`, 
      'Actor' AS `__cg_from_label`, 
      'Actor' AS `__cg_to_label`, 
      e.evt_id AS `__cg_rid_0`, 
      e.dst_actor AS `__cg_from_0`, 
      e.src_actor AS `__cg_to_0`, 
      e.src_actor AS `__cg_start_0`, 
      e.dst_actor AS `__cg_end_0`, 
      'Actor' AS `__cg_start_label`, 
      'Actor' AS `__cg_end_label`, 
      concat('RECEIVED:Actor:Actor:', CAST(e.evt_id AS STRING)) AS `__cg_key`
FROM db_denorm_selfloop.events AS e
WHERE e.dst_actor <> e.src_actor

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
      concat('SENT:Actor:Actor:', CAST(e.evt_id AS STRING)) AS `__cg_key`
FROM db_denorm_selfloop.events AS e

UNION ALL
SELECT 
      'SENT' AS `__cg_type`, 
      'Actor' AS `__cg_from_label`, 
      'Actor' AS `__cg_to_label`, 
      e.evt_id AS `__cg_rid_0`, 
      e.src_actor AS `__cg_from_0`, 
      e.dst_actor AS `__cg_to_0`, 
      e.dst_actor AS `__cg_start_0`, 
      e.src_actor AS `__cg_end_0`, 
      'Actor' AS `__cg_start_label`, 
      'Actor' AS `__cg_end_label`, 
      concat('SENT:Actor:Actor:', CAST(e.evt_id AS STRING)) AS `__cg_key`
FROM db_denorm_selfloop.events AS e
WHERE e.src_actor <> e.dst_actor
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
      v0.name AS `a.name`, 
      v2.name AS `b.name`
FROM __cg_nodes_Actor AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.actor_id
JOIN __cg_nodes_Actor AS v2 ON v1.__cg_end_0 = v2.actor_id

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]