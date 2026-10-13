WITH RECURSIVE v1_both AS (
SELECT 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.since AS "since", 
      e.follower_id AS "__cg_start_0", 
      e.followed_id AS "__cg_end_0"
FROM test_integration.follows AS e

UNION ALL
SELECT 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.since AS "since", 
      e.followed_id AS "__cg_start_0", 
      e.follower_id AS "__cg_end_0"
FROM test_integration.follows AS e
WHERE e.follower_id <> e.followed_id
), 
vlp_v1_bfs AS (
    SELECT DISTINCT start_node.user_id AS start_id, start_node.user_id AS node, CAST(0 AS UInt32) AS depth, CAST(1 AS UInt256) AS paths, CAST(1 AS UInt8) AS new
    FROM test_integration.users AS start_node
    WHERE start_node.user_id IS NOT NULL AND (start_node.name = 'Alice')
    UNION ALL
    SELECT f.start_id AS start_id, end_node.user_id AS node, CAST(f.depth + 1 AS UInt32) AS depth, CAST(sum(f.paths) AS UInt256) AS paths, CAST(1 AS UInt8) AS new
    FROM vlp_v1_bfs AS f
    JOIN v1_both AS rel ON rel.__cg_start_0 = f.node
    JOIN test_integration.users AS end_node ON end_node.user_id = rel.__cg_end_0
    WHERE f.new = 1
      AND (f.start_id, end_node.user_id) NOT IN (SELECT start_id, node FROM vlp_v1_bfs)
      AND f.start_id NOT IN (SELECT start_id FROM vlp_v1_bfs GROUP BY start_id HAVING countIf(node IN (SELECT end_node.user_id FROM test_integration.users AS end_node WHERE (end_node.name = 'Diana'))) >= (SELECT count(DISTINCT end_node.user_id) FROM (SELECT end_node.user_id FROM test_integration.users AS end_node WHERE (end_node.name = 'Diana')) AS end_node))
    GROUP BY f.start_id, end_node.user_id, f.depth
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
      count(*) AS "path_count"
FROM vlp_v1_path AS v1
JOIN test_integration.users AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
WHERE (v0.name = 'Alice' AND v2.name = 'Diana')

-- column path_count: Value [("path_count", "path_count")]