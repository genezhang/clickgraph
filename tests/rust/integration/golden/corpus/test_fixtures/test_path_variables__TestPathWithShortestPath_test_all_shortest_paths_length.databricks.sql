WITH RECURSIVE vlp_v1_bfs AS (
    SELECT vlp_v1_seed.start_id AS start_id, vlp_v1_seed.start_id AS node, CAST(0 AS INT) AS depth, CAST(1 AS INT) AS new
    FROM (SELECT DISTINCT start_node.user_id AS start_id FROM test_integration.users AS start_node WHERE start_node.user_id IS NOT NULL AND (start_node.name = 'Alice')) AS vlp_v1_seed
    UNION ALL
    (SELECT vlp_v1_frontier.start_id AS start_id, end_node.user_id AS node, CAST(vlp_v1_frontier.depth + 1 AS INT) AS depth,
      CAST(1 AS INT) AS new
    FROM vlp_v1_bfs AS vlp_v1_frontier
    JOIN test_integration.follows AS rel ON rel.follower_id = vlp_v1_frontier.node
    JOIN test_integration.users AS end_node ON end_node.user_id = rel.followed_id
    WHERE vlp_v1_frontier.new = 1
      AND (vlp_v1_frontier.start_id, end_node.user_id) NOT IN (SELECT start_id, node FROM vlp_v1_bfs)
    UNION ALL
    SELECT start_id, node, depth, CAST(0 AS INT) AS new FROM (
      SELECT DISTINCT start_id, node, depth FROM vlp_v1_bfs
      WHERE start_id IN (SELECT start_id FROM vlp_v1_bfs WHERE new = 1)
    ) AS vlp_v1_reached)
), 
vlp_v1_path AS (
    SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v1_bfs
    WHERE new = 1 AND depth >= 1
      AND node IN (SELECT end_node.user_id FROM test_integration.users AS end_node WHERE (end_node.name = 'Diana'))
)
SELECT 
      count(*) AS `path_count`, 
      min(v1.hop_count) AS `min_length`
FROM vlp_v1_path AS v1
JOIN test_integration.users AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
WHERE (v0.name = 'Alice' AND v2.name = 'Diana')

-- column path_count: Value [("path_count", "path_count")]
-- column min_length: Value [("min_length", "min_length")]