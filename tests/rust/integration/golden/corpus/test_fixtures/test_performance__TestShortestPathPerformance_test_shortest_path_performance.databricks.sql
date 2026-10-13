WITH RECURSIVE vlp_v1_bfs AS (
    SELECT DISTINCT start_node.user_id AS start_id, start_node.user_id AS node, CAST(0 AS INT) AS depth, CAST(1 AS INT) AS new
    FROM test_integration.users AS start_node
    WHERE start_node.user_id IS NOT NULL AND (start_node.name = 'Alice')
    UNION ALL
    (SELECT DISTINCT f.start_id AS start_id, end_node.user_id AS node, CAST(f.depth + 1 AS INT) AS depth, CAST(1 AS INT) AS new
    FROM vlp_v1_bfs AS f
    JOIN test_integration.follows AS rel ON rel.follower_id = f.node
    JOIN test_integration.users AS end_node ON end_node.user_id = rel.followed_id
    WHERE f.new = 1
      AND (f.start_id, end_node.user_id) NOT IN (SELECT start_id, node FROM vlp_v1_bfs)
    UNION ALL
    SELECT start_id, node, depth, CAST(0 AS INT) AS new
    FROM vlp_v1_bfs
    WHERE start_id IN (SELECT start_id FROM vlp_v1_bfs WHERE new = 1))
), 
vlp_v1_path AS (
    SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v1_bfs
    WHERE new = 1 AND depth >= 1
      AND node IN (SELECT end_node.user_id FROM test_integration.users AS end_node WHERE (end_node.name = 'Diana'))
)
SELECT 
      v1.hop_count AS `path_length`
FROM vlp_v1_path AS v1
JOIN test_integration.users AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
WHERE (v0.name = 'Alice' AND v2.name = 'Diana')

-- column path_length: Value [("path_length", "path_length")]