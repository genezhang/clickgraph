WITH pattern_union_t1600 AS (
(SELECT 'Actor' AS start_type, string(db_denorm_selfloop.events.src_actor) as start_id, string(db_denorm_selfloop.events.dst_actor) as end_id, 'Actor' AS end_type, array('RECEIVED') as path_relationships, array(to_json(struct(db_denorm_selfloop.events.channel AS channel, db_denorm_selfloop.events.evt_id AS evt_id))) as rel_properties, to_json(struct(db_denorm_selfloop.events.src_actor, db_denorm_selfloop.events.src_name)) as start_properties, to_json(struct(db_denorm_selfloop.events.dst_actor, db_denorm_selfloop.events.dst_name)) as end_properties, db_denorm_selfloop.events.channel AS channel, db_denorm_selfloop.events.evt_id AS evt_id, NULL AS weight FROM db_denorm_selfloop.events)
UNION ALL
(SELECT 'Actor' AS start_type, string(db_denorm_selfloop.events.src_actor) as start_id, string(db_denorm_selfloop.events.dst_actor) as end_id, 'Actor' AS end_type, array('SENT') as path_relationships, array(to_json(struct(db_denorm_selfloop.events.evt_id AS evt_id, db_denorm_selfloop.events.weight AS weight))) as rel_properties, to_json(struct(db_denorm_selfloop.events.src_actor, db_denorm_selfloop.events.src_name)) as start_properties, to_json(struct(db_denorm_selfloop.events.dst_actor, db_denorm_selfloop.events.dst_name)) as end_properties, NULL AS channel, db_denorm_selfloop.events.evt_id AS evt_id, db_denorm_selfloop.events.weight AS weight FROM db_denorm_selfloop.events)
)
SELECT 
      t0.start_properties AS `_start_properties`, 
      t0.end_properties AS `_end_properties`, 
      t0.rel_properties AS `_rel_properties`, 
      element_at(t0.path_relationships, 1) AS `__rel_type__`, 
      t0.start_id AS `__start_id__`, 
      t0.end_id AS `__end_id__`, 
      t0.start_type AS `__start_label__`, 
      t0.end_type AS `__end_label__`
FROM pattern_union_t1600 AS t0
LIMIT 5