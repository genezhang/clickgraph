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
vlp_v1_near AS (
    SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v1_bfs
    WHERE new = 1 AND depth >= 1
      AND node IN (SELECT end_node.user_id FROM test_integration.users AS end_node WHERE (end_node.name = 'Diana'))
), 
vlp_v1_path_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(struct(rel.follower_id, rel.followed_id)) as path_edges
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.name = 'Alice')
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.follower_id, rel.followed_id))) as path_edges
    FROM vlp_v1_path_inner vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2147483647
      AND NOT array_contains(vp.path_edges, struct(rel.follower_id, rel.followed_id))
),
vlp_v1_path AS (
    SELECT * FROM vlp_v1_path_inner WHERE hop_count >= 2
), 
vlp_v1_shortest AS (
    SELECT start_id, end_id, hop_count FROM (
        SELECT v1.start_id AS start_id, v1.end_id AS end_id, v1.hop_count AS hop_count
        FROM vlp_v1_near AS v1
        WHERE (v1.hop_count >= 2)
    )
    UNION ALL
    SELECT start_id, end_id, hop_count FROM (
        SELECT v1.start_id AS start_id, v1.end_id AS end_id, v1.hop_count AS hop_count, ROW_NUMBER() OVER (PARTITION BY v1.start_id, v1.end_id ORDER BY v1.hop_count) AS shortest
        FROM vlp_v1_path AS v1
        WHERE (v1.hop_count >= 2) AND (v1.start_id, v1.end_id) IN (SELECT v1.start_id AS start_id, v1.end_id AS end_id FROM vlp_v1_near AS v1 WHERE NOT coalesce((v1.hop_count >= 2), false)) AND v1.start_id <> v1.end_id
    ) WHERE shortest = 1
)
SELECT 
      v0.name AS `a.name`, 
      v2.name AS `b.name`
FROM vlp_v1_shortest AS v1
JOIN test_integration.users AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
WHERE (v0.name = 'Alice' AND v2.name = 'Diana')

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]