WITH RECURSIVE with_w1 AS (
SELECT 
      v0.user_id AS "v0__user_id", 
      v0.name AS "p2_v0_name"
FROM test_integration.users AS v0
WHERE v0.name = 'Eve'
), 
optional_d2 AS (
SELECT DISTINCT 
      w1.v0__user_id AS "v0__user_id"
FROM with_w1 AS w1
), 
vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [tuple(rel.follower_id, rel.followed_id)] as path_edges
    FROM test_integration.users AS start_node
    JOIN test_integration.follows AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      d2.v0__user_id AS "id"
FROM optional_d2 AS d2))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.follower_id, rel.followed_id)]) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.follower_id, rel.followed_id))
), 
optional_o3 AS (
SELECT 
      d2.v0__user_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id"
FROM vlp_v1_path AS v1
JOIN optional_d2 AS d2 ON v1.start_id = d2.v0__user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
)
SELECT 
      w1.p2_v0_name AS "a.name", 
      count(o3.v2__user_id) AS "reachable"
FROM with_w1 AS w1
LEFT JOIN optional_o3 AS o3 ON w1.v0__user_id = o3.v0__user_id
GROUP BY w1.p2_v0_name

-- column a.name: Value [("a.name", "a.name")]
-- column reachable: Value [("reachable", "reachable")]