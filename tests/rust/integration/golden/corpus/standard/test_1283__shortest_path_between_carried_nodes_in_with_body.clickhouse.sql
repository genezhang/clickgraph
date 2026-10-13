WITH RECURSIVE with_w1 AS (
SELECT 
      v0.user_id AS "v2__user_id", 
      v1.user_id AS "v3__user_id", 
      v1.user_id AS "p2_v3_user_id"
FROM test_integration.users_test AS v0
CROSS JOIN test_integration.users_test AS v1
WHERE (v0.user_id = 1 AND v1.user_id IN [2, 7, 15])
), 
v4_both AS (
SELECT 
      e.follow_date AS "follow_date", 
      e.follow_id AS "follow_id", 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.follower_id AS "__cg_start_0", 
      e.followed_id AS "__cg_end_0"
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      e.follow_date AS "follow_date", 
      e.follow_id AS "follow_id", 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.followed_id AS "__cg_start_0", 
      e.follower_id AS "__cg_end_0"
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id
), 
vlp_v4_bfs AS (
    SELECT DISTINCT start_node.user_id AS start_id, start_node.user_id AS node, CAST(0 AS UInt32) AS depth, CAST(1 AS UInt8) AS new
    FROM test_integration.users_test AS start_node
    WHERE start_node.user_id IS NOT NULL AND (start_node.user_id IN (SELECT DISTINCT 
      w1.v2__user_id AS "id"
FROM with_w1 AS w1))
    UNION ALL
    SELECT DISTINCT f.start_id AS start_id, end_node.user_id AS node, CAST(f.depth + 1 AS UInt32) AS depth, CAST(1 AS UInt8) AS new
    FROM vlp_v4_bfs AS f
    JOIN v4_both AS rel ON rel.__cg_start_0 = f.node
    JOIN test_integration.users_test AS end_node ON end_node.user_id = rel.__cg_end_0
    WHERE f.new = 1
      AND f.depth < 3
      AND (f.start_id, end_node.user_id) NOT IN (SELECT start_id, node FROM vlp_v4_bfs)
      AND f.start_id NOT IN (SELECT start_id FROM vlp_v4_bfs GROUP BY start_id HAVING countIf(node IN (SELECT end_node.user_id FROM test_integration.users_test AS end_node WHERE (end_node.user_id IN (SELECT DISTINCT 
      w1.v3__user_id AS "id"
FROM with_w1 AS w1)))) >= (SELECT count(DISTINCT end_node.user_id) FROM (SELECT end_node.user_id FROM test_integration.users_test AS end_node WHERE (end_node.user_id IN (SELECT DISTINCT 
      w1.v3__user_id AS "id"
FROM with_w1 AS w1))) AS end_node))
    UNION ALL
    SELECT start_id, node, depth, CAST(0 AS UInt8) AS new
    FROM vlp_v4_bfs
    WHERE start_id IN (SELECT start_id FROM vlp_v4_bfs WHERE new = 1)
), 
vlp_v4_path AS (
    SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v4_bfs
    WHERE new = 1 AND depth >= 1
      AND node IN (SELECT end_node.user_id FROM test_integration.users_test AS end_node WHERE (end_node.user_id IN (SELECT DISTINCT 
      w1.v3__user_id AS "id"
FROM with_w1 AS w1)))
), 
with_w5 AS (
SELECT 
      minOrNull(v4.hop_count) AS "v6", 
      w1.v3__user_id AS "v7__user_id", 
      w1.p2_v3_user_id AS "p2_v7_user_id"
FROM vlp_v4_path AS v4
JOIN with_w1 AS w1 ON v4.start_id = w1.v2__user_id AND v4.end_id = w1.v3__user_id
GROUP BY w1.v3__user_id, w1.p2_v3_user_id
)
SELECT 
      w5.p2_v7_user_id AS "fid", 
      w5.v6 AS "d"
FROM with_w5 AS w5

-- column fid: Value [("fid", "fid")]
-- column d: Value [("d", "d")]