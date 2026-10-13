WITH RECURSIVE v2_both AS (
SELECT 
      e.Person1Id AS "Person1Id", 
      e.Person2Id AS "Person2Id", 
      e.creationDate AS "creationDate", 
      e.Person1Id AS "__cg_start_0", 
      e.Person2Id AS "__cg_end_0"
FROM ldbc.Person_knows_Person AS e

UNION ALL
SELECT 
      e.Person1Id AS "Person1Id", 
      e.Person2Id AS "Person2Id", 
      e.creationDate AS "creationDate", 
      e.Person2Id AS "__cg_start_0", 
      e.Person1Id AS "__cg_end_0"
FROM ldbc.Person_knows_Person AS e
WHERE e.Person1Id <> e.Person2Id
), 
vlp_v2_bfs AS (
    SELECT DISTINCT start_node.id AS start_id, start_node.id AS node, CAST(0 AS UInt32) AS depth, start_node.id AS parent, CAST(1 AS UInt8) AS new
    FROM ldbc.Person AS start_node
    WHERE start_node.id IS NOT NULL AND (start_node.id = 14)
    UNION ALL
    SELECT f.start_id AS start_id, end_node.id AS node, CAST(f.depth + 1 AS UInt32) AS depth, min(f.node) AS parent, CAST(1 AS UInt8) AS new
    FROM vlp_v2_bfs AS f
    JOIN v2_both AS rel ON rel.__cg_start_0 = f.node
    JOIN ldbc.Person AS end_node ON end_node.id = rel.__cg_end_0
    WHERE f.new = 1
      AND (f.start_id, end_node.id) NOT IN (SELECT start_id, node FROM vlp_v2_bfs)
      AND f.start_id NOT IN (SELECT start_id FROM vlp_v2_bfs GROUP BY start_id HAVING countIf(node IN (SELECT end_node.id FROM ldbc.Person AS end_node WHERE (end_node.id = 16))) >= (SELECT count(DISTINCT end_node.id) FROM (SELECT end_node.id FROM ldbc.Person AS end_node WHERE (end_node.id = 16)) AS end_node))
    GROUP BY f.start_id, end_node.id, f.depth
    UNION ALL
    SELECT start_id, node, depth, parent, CAST(0 AS UInt8) AS new
    FROM vlp_v2_bfs
    WHERE start_id IN (SELECT start_id FROM vlp_v2_bfs WHERE new = 1)
), 
vlp_v2_walk AS (
    SELECT start_id, node, depth, node AS end_id, depth AS hop_count, arraySlice([node], 1, 0) AS path_nodes, CAST([], 'Array(String)') AS path_edges, CAST([], 'Array(Map(String, Dynamic))') AS path_node_values, CAST([], 'Array(Map(String, Dynamic))') AS path_rel_values, parent, CAST(1 AS UInt8) AS level, CAST(depth >= 1
      AND node IN (SELECT end_node.id FROM ldbc.Person AS end_node WHERE (end_node.id = 16)) AS UInt8) AS frontier
    FROM vlp_v2_bfs
    WHERE new = 1
    UNION ALL
    SELECT start_id, node, depth, end_id, hop_count, path_nodes, path_edges, path_node_values, path_rel_values, parent, level, frontier FROM (
        SELECT w.start_id AS start_id, lv.parent AS node, CAST(w.depth - 1 AS UInt32) AS depth, w.end_id AS end_id, w.hop_count AS hop_count,
          arrayConcat([end_node.id], w.path_nodes) AS path_nodes,
          arrayConcat([toString(tuple(rel.Person1Id, rel.Person2Id))], w.path_edges) AS path_edges,
          arrayConcat([map('elementId', CAST(concat('Person:', toString(end_node.id), '-'), 'Dynamic'), 'labels', CAST(['Person'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('birthday', CAST(end_node.birthday, 'Dynamic'), 'browserUsed', CAST(end_node.browserUsed, 'Dynamic'), 'creationDate', CAST(end_node.creationDate, 'Dynamic'), 'email', CAST(end_node.email, 'Dynamic'), 'firstName', CAST(end_node.firstName, 'Dynamic'), 'gender', CAST(end_node.gender, 'Dynamic'), 'id', CAST(end_node.id, 'Dynamic'), 'lastName', CAST(end_node.lastName, 'Dynamic'), 'locationIP', CAST(end_node.locationIP, 'Dynamic'), 'speaks', CAST(end_node.speaks, 'Dynamic'))), 'Dynamic'))], w.path_node_values) AS path_node_values,
          arrayConcat([map('elementId', CAST(concat('KNOWS:', toString(rel.Person1Id), '->', toString(rel.Person2Id), '-'), 'Dynamic'), 'startNodeElementId', CAST(concat('Person:', toString(rel.Person1Id), '-'), 'Dynamic'), 'endNodeElementId', CAST(concat('Person:', toString(rel.Person2Id), '-'), 'Dynamic'), 'type', CAST('KNOWS', 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('creationDate', CAST(rel.creationDate, 'Dynamic'))), 'Dynamic'))], w.path_rel_values) AS path_rel_values, w.parent AS parent,
          CAST(0 AS UInt8) AS level, CAST(1 AS UInt8) AS frontier,
          ROW_NUMBER() OVER (PARTITION BY w.start_id, w.end_id ORDER BY toString(tuple(rel.Person1Id, rel.Person2Id))) AS pick
        FROM vlp_v2_walk AS w
        JOIN (SELECT start_id, node, parent FROM vlp_v2_walk WHERE level = 1) AS lv
          ON lv.start_id = w.start_id AND lv.node = w.node
        JOIN (SELECT * FROM v2_both WHERE __cg_end_0 IN (SELECT node FROM vlp_v2_walk WHERE frontier = 1 AND depth > 0)) AS rel
          ON rel.__cg_start_0 = lv.parent AND rel.__cg_end_0 = w.node
        JOIN ldbc.Person AS end_node ON end_node.id = w.node
        WHERE w.frontier = 1 AND w.depth > 0
    ) WHERE pick = 1
    UNION ALL
    SELECT start_id, node, depth, end_id, hop_count, path_nodes, CAST([], 'Array(String)') AS path_edges, CAST([], 'Array(Map(String, Dynamic))') AS path_node_values, CAST([], 'Array(Map(String, Dynamic))') AS path_rel_values, parent, level, CAST(0 AS UInt8) AS frontier
    FROM vlp_v2_walk
    WHERE level = 1 AND start_id IN (SELECT start_id FROM vlp_v2_walk WHERE frontier = 1 AND depth > 1)
), 
vlp_v2_path AS (
    SELECT w.start_id AS start_id, w.end_id AS end_id, w.hop_count AS hop_count, arrayConcat([w.start_id], w.path_nodes) AS path_nodes, w.path_edges AS path_edges, arrayConcat([map('elementId', CAST(concat('Person:', toString(start_node.id), '-'), 'Dynamic'), 'labels', CAST(['Person'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('birthday', CAST(start_node.birthday, 'Dynamic'), 'browserUsed', CAST(start_node.browserUsed, 'Dynamic'), 'creationDate', CAST(start_node.creationDate, 'Dynamic'), 'email', CAST(start_node.email, 'Dynamic'), 'firstName', CAST(start_node.firstName, 'Dynamic'), 'gender', CAST(start_node.gender, 'Dynamic'), 'id', CAST(start_node.id, 'Dynamic'), 'lastName', CAST(start_node.lastName, 'Dynamic'), 'locationIP', CAST(start_node.locationIP, 'Dynamic'), 'speaks', CAST(start_node.speaks, 'Dynamic'))), 'Dynamic'))], w.path_node_values) AS path_node_values, w.path_rel_values AS path_rel_values
    FROM vlp_v2_walk AS w
    JOIN ldbc.Person AS start_node ON start_node.id = w.start_id
    WHERE w.frontier = 1 AND w.depth = 0
)
SELECT 
      caseWithExpression((v0.id IS NULL OR v1.id IS NULL), true, 0 - 1, v2.hop_count) AS "shortestPathLength"
FROM vlp_v2_path AS v2
JOIN ldbc.Person AS v0 ON v2.start_id = v0.id
JOIN ldbc.Person AS v1 ON v2.end_id = v1.id
WHERE (v0.id = 14 AND v1.id = 16)

-- column shortestPathLength: Value [("shortestPathLength", "shortestPathLength")]