WITH RECURSIVE vlp_v1_bfs AS (
    SELECT DISTINCT start_node.user_id AS start_id, start_node.user_id AS node, CAST(0 AS UInt32) AS depth, CAST(1 AS UInt256) AS paths, CAST(1 AS UInt8) AS new
    FROM test_integration.users AS start_node
    WHERE start_node.user_id IS NOT NULL AND (start_node.name = 'Alice')
    UNION ALL
    SELECT vlp_v1_frontier.start_id AS start_id, end_node.user_id AS node, CAST(vlp_v1_frontier.depth + 1 AS UInt32) AS depth, CAST(sum(vlp_v1_frontier.paths) AS UInt256) AS paths, CAST(1 AS UInt8) AS new
    FROM vlp_v1_bfs AS vlp_v1_frontier
    JOIN test_integration.follows AS rel ON rel.follower_id = vlp_v1_frontier.node
    JOIN test_integration.users AS end_node ON end_node.user_id = rel.followed_id
    WHERE vlp_v1_frontier.new = 1
      AND (vlp_v1_frontier.start_id, end_node.user_id) NOT IN (SELECT start_id, node FROM vlp_v1_bfs)
      AND vlp_v1_frontier.start_id NOT IN (SELECT start_id FROM vlp_v1_bfs GROUP BY start_id HAVING countIf(node IN (SELECT end_node.user_id FROM test_integration.users AS end_node WHERE (end_node.name = 'Diana'))) >= (SELECT count(DISTINCT end_node.user_id) FROM (SELECT end_node.user_id FROM test_integration.users AS end_node WHERE (end_node.name = 'Diana')) AS end_node))
    GROUP BY vlp_v1_frontier.start_id, end_node.user_id, vlp_v1_frontier.depth
    UNION ALL
    SELECT start_id, node, depth, paths, CAST(0 AS UInt8) AS new
    FROM vlp_v1_bfs
    WHERE start_id IN (SELECT start_id FROM vlp_v1_bfs WHERE new = 1)
), 
vlp_v1_path AS (
    SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v1_bfs ARRAY JOIN range(accurateCast(paths, 'UInt64')) AS copy
    WHERE new = 1 AND depth >= 1
      AND node IN (SELECT end_node.user_id FROM test_integration.users AS end_node WHERE (end_node.name = 'Diana'))
)
SELECT 
      count(*) AS "path_count", 
      minOrNull(v1.hop_count) AS "min_length"
FROM vlp_v1_path AS v1
JOIN test_integration.users AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
WHERE (v0.name = 'Alice' AND v2.name = 'Diana')

-- column path_count: Value [("path_count", "path_count")]
-- column min_length: Value [("min_length", "min_length")]