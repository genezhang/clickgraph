WITH RECURSIVE vlp_v1_nodes AS (
SELECT 
      'Member' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      concat('Member', ':', CAST(e.id AS STRING)) AS `__cg_key`
FROM mt606.members AS e
), 
v1_rels AS (
SELECT 
      'HELPS' AS `__cg_type`, 
      'Member' AS `__cg_from_label`, 
      'Member' AS `__cg_to_label`, 
      e.from_id AS `__cg_rid_0`, 
      e.to_id AS `__cg_rid_1`, 
      e.from_id AS `__cg_from_0`, 
      e.to_id AS `__cg_to_0`, 
      e.from_id AS `__cg_start_0`, 
      e.to_id AS `__cg_end_0`, 
      'Member' AS `__cg_start_label`, 
      'Member' AS `__cg_end_label`, 
      concat('HELPS:Member:Member:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM mt606.helps AS e

UNION ALL
SELECT 
      'MENTORS' AS `__cg_type`, 
      'Member' AS `__cg_from_label`, 
      'Member' AS `__cg_to_label`, 
      e.from_id AS `__cg_rid_0`, 
      e.to_id AS `__cg_rid_1`, 
      e.from_id AS `__cg_from_0`, 
      e.to_id AS `__cg_to_0`, 
      e.from_id AS `__cg_start_0`, 
      e.to_id AS `__cg_end_0`, 
      'Member' AS `__cg_start_label`, 
      'Member' AS `__cg_end_label`, 
      concat('MENTORS:Member:Member:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM mt606.mentors AS e
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes
    FROM vlp_v1_nodes AS start_node
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v1_trails
    WHERE hop_count >= 1
)
SELECT 
      v0.id AS `a.id`, 
      v2.id AS `b.id`
FROM vlp_v1_path AS v1
JOIN mt606.members AS v0 ON v1.start_id = v0.id
JOIN mt606.members AS v2 ON v1.end_id = v2.id
