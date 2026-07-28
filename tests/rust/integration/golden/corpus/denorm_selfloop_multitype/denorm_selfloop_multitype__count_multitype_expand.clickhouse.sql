WITH pattern_union_r AS (
SELECT 'Actor' AS start_type, toString(db_denorm_selfloop.events.src_actor) as start_id, toString(db_denorm_selfloop.events.dst_actor) as end_id, 'Actor' AS end_type, ['RECEIVED'] as path_relationships, [formatRowNoNewline('JSONEachRow', db_denorm_selfloop.events.channel AS channel, db_denorm_selfloop.events.evt_id AS evt_id)] as rel_properties, formatRowNoNewline('JSONEachRow', db_denorm_selfloop.events.src_actor, db_denorm_selfloop.events.src_name) as start_properties, formatRowNoNewline('JSONEachRow', db_denorm_selfloop.events.dst_actor, db_denorm_selfloop.events.dst_name) as end_properties, db_denorm_selfloop.events.channel AS channel, db_denorm_selfloop.events.evt_id AS evt_id, NULL AS weight FROM db_denorm_selfloop.events
UNION ALL
SELECT 'Actor' AS start_type, toString(db_denorm_selfloop.events.src_actor) as start_id, toString(db_denorm_selfloop.events.dst_actor) as end_id, 'Actor' AS end_type, ['SENT'] as path_relationships, [formatRowNoNewline('JSONEachRow', db_denorm_selfloop.events.evt_id AS evt_id, db_denorm_selfloop.events.weight AS weight)] as rel_properties, formatRowNoNewline('JSONEachRow', db_denorm_selfloop.events.src_actor, db_denorm_selfloop.events.src_name) as start_properties, formatRowNoNewline('JSONEachRow', db_denorm_selfloop.events.dst_actor, db_denorm_selfloop.events.dst_name) as end_properties, NULL AS channel, db_denorm_selfloop.events.evt_id AS evt_id, db_denorm_selfloop.events.weight AS weight FROM db_denorm_selfloop.events
)
SELECT 
      count(*) AS "cnt"
FROM pattern_union_r AS r
