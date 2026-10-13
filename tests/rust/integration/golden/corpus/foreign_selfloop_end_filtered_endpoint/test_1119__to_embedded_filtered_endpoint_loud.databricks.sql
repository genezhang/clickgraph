WITH RECURSIVE vlp_v1_nodes AS (
SELECT 
      'Person' AS `__cg_label`, 
      e.pid AS `__cg_id_0`, 
      concat('Person', ':', CAST(e.pid AS STRING)) AS `__cg_key`
FROM testdb.people AS e
WHERE ((e.pid > 0))
), 
v1_rels AS (
SELECT 
      'REPORTS_TO' AS `__cg_type`, 
      'Person' AS `__cg_from_label`, 
      'Person' AS `__cg_to_label`, 
      e.mgr_id AS `__cg_rid_0`, 
      e.emp_id AS `__cg_rid_1`, 
      e.mgr_id AS `__cg_from_0`, 
      e.emp_id AS `__cg_to_0`, 
      e.mgr_id AS `__cg_start_0`, 
      e.emp_id AS `__cg_end_0`, 
      'Person' AS `__cg_start_label`, 
      'Person' AS `__cg_end_label`, 
      concat('REPORTS_TO:Person:Person:', to_json(named_struct('_1', CAST(e.mgr_id AS STRING), '_2', CAST(e.emp_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM testdb.reports AS e
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes
    FROM vlp_v1_nodes AS start_node
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes
    FROM vlp_v1_trails
    WHERE hop_count >= 1
)
SELECT 
      count(*) AS `count(*)`
FROM vlp_v1_path AS v1
JOIN testdb.people AS v0 ON v1.start_id = v0.pid
JOIN testdb.people AS v2 ON v1.end_id = v2.pid
WHERE (((v0.pid > 0)) AND ((v2.pid > 0)))
