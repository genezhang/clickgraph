WITH RECURSIVE v2_both AS (
SELECT 
      e.Person1Id AS `Person1Id`, 
      e.Person2Id AS `Person2Id`, 
      e.creationDate AS `creationDate`, 
      e.Person1Id AS `__cg_start_0`, 
      e.Person2Id AS `__cg_end_0`
FROM ldbc.Person_knows_Person AS e

UNION ALL
SELECT 
      e.Person1Id AS `Person1Id`, 
      e.Person2Id AS `Person2Id`, 
      e.creationDate AS `creationDate`, 
      e.Person2Id AS `__cg_start_0`, 
      e.Person1Id AS `__cg_end_0`
FROM ldbc.Person_knows_Person AS e
WHERE e.Person1Id <> e.Person2Id
), 
vlp_v2_bfs AS (
    SELECT s.start_id AS start_id, s.start_id AS node, CAST(0 AS INT) AS depth, slice(array(s.start_id), 1, 0) AS path_nodes, CAST(array() AS ARRAY<STRING>) AS path_edges, CAST(array() AS ARRAY<VARIANT>) AS path_node_values, CAST(array() AS ARRAY<VARIANT>) AS path_rel_values, CAST(1 AS INT) AS new
    FROM (SELECT DISTINCT start_node.id AS start_id FROM ldbc.Person AS start_node WHERE start_node.id IS NOT NULL AND (start_node.id = 14)) AS s
    UNION ALL
    (SELECT f.start_id AS start_id, end_node.id AS node, CAST(f.depth + 1 AS INT) AS depth,
      concat(f.path_nodes, array(end_node.id)) AS path_nodes,
      concat(f.path_edges, array(to_json(named_struct('_1', CAST(rel.Person1Id AS STRING), '_2', CAST(rel.Person2Id AS STRING)), map('ignoreNullFields', 'false')))) AS path_edges,
      concat(f.path_node_values, array(to_variant_object(map('elementId', CAST(concat('Person:', CAST(end_node.id AS STRING), '-') AS VARIANT), 'labels', CAST(array('Person') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('birthday', CAST(end_node.birthday AS VARIANT), 'browserUsed', CAST(end_node.browserUsed AS VARIANT), 'creationDate', CAST(end_node.creationDate AS VARIANT), 'email', CAST(end_node.email AS VARIANT), 'firstName', CAST(end_node.firstName AS VARIANT), 'gender', CAST(end_node.gender AS VARIANT), 'id', CAST(end_node.id AS VARIANT), 'lastName', CAST(end_node.lastName AS VARIANT), 'locationIP', CAST(end_node.locationIP AS VARIANT), 'speaks', CAST(end_node.speaks AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))))) AS path_node_values,
      concat(f.path_rel_values, array(to_variant_object(map('elementId', CAST(concat('KNOWS:', CAST(rel.Person1Id AS STRING), '->', CAST(rel.Person2Id AS STRING), '-') AS VARIANT), 'startNodeElementId', CAST(concat('Person:', CAST(rel.Person1Id AS STRING), '-') AS VARIANT), 'endNodeElementId', CAST(concat('Person:', CAST(rel.Person2Id AS STRING), '-') AS VARIANT), 'type', CAST('KNOWS' AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('creationDate', CAST(rel.creationDate AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))))) AS path_rel_values,
      CAST(1 AS INT) AS new
    FROM vlp_v2_bfs AS f
    JOIN v2_both AS rel ON rel.__cg_start_0 = f.node
    JOIN ldbc.Person AS end_node ON end_node.id = rel.__cg_end_0
    WHERE f.new = 1
      AND (f.start_id, end_node.id) NOT IN (SELECT start_id, node FROM vlp_v2_bfs)
    UNION ALL
    SELECT start_id, node, depth, slice(array(node), 1, 0) AS path_nodes, CAST(array() AS ARRAY<STRING>) AS path_edges, CAST(array() AS ARRAY<VARIANT>) AS path_node_values, CAST(array() AS ARRAY<VARIANT>) AS path_rel_values, CAST(0 AS INT) AS new FROM (
      SELECT DISTINCT start_id, node, depth FROM vlp_v2_bfs
      WHERE start_id IN (SELECT start_id FROM vlp_v2_bfs WHERE new = 1)
    ) AS reached)
), 
vlp_v2_walk AS (
    SELECT start_id, node AS end_id, depth AS hop_count, path_nodes, path_edges, path_node_values, path_rel_values
    FROM (SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id, node ORDER BY reverse(path_nodes), reverse(path_edges)) AS pick
        FROM (SELECT * FROM vlp_v2_bfs WHERE new = 1 AND depth >= 1
      AND node IN (SELECT end_node.id FROM ldbc.Person AS end_node WHERE (end_node.id = 16))) AS found
    ) AS picked WHERE pick = 1) AS w
), 
vlp_v2_path AS (
    SELECT w.start_id AS start_id, w.end_id AS end_id, w.hop_count AS hop_count, concat(array(w.start_id), w.path_nodes) AS path_nodes, w.path_edges AS path_edges, concat(array(to_variant_object(map('elementId', CAST(concat('Person:', CAST(start_node.id AS STRING), '-') AS VARIANT), 'labels', CAST(array('Person') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('birthday', CAST(start_node.birthday AS VARIANT), 'browserUsed', CAST(start_node.browserUsed AS VARIANT), 'creationDate', CAST(start_node.creationDate AS VARIANT), 'email', CAST(start_node.email AS VARIANT), 'firstName', CAST(start_node.firstName AS VARIANT), 'gender', CAST(start_node.gender AS VARIANT), 'id', CAST(start_node.id AS VARIANT), 'lastName', CAST(start_node.lastName AS VARIANT), 'locationIP', CAST(start_node.locationIP AS VARIANT), 'speaks', CAST(start_node.speaks AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))), w.path_node_values) AS path_node_values, w.path_rel_values AS path_rel_values
    FROM vlp_v2_walk AS w
    JOIN ldbc.Person AS start_node ON start_node.id = w.start_id
    WHERE true
)
SELECT 
      CASE WHEN (v0.id IS NULL OR v1.id IS NULL) = true THEN 0 - 1 ELSE v2.hop_count END AS `shortestPathLength`
FROM vlp_v2_path AS v2
JOIN ldbc.Person AS v0 ON v2.start_id = v0.id
JOIN ldbc.Person AS v1 ON v2.end_id = v1.id
WHERE (v0.id = 14 AND v1.id = 16)

-- column shortestPathLength: Value [("shortestPathLength", "shortestPathLength")]