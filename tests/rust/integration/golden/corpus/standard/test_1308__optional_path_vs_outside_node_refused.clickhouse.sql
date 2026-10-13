WITH RECURSIVE with_w1 AS (
SELECT 
      v0.user_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
), 
optional_d2 AS (
SELECT DISTINCT 
      w1.v0__user_id AS "v0__user_id", 
      w1.v2__user_id AS "v2__user_id"
FROM with_w1 AS w1
), 
vlp_v3_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      d2.v2__user_id AS "id"
FROM optional_d2 AS d2))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v3_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
optional_o3 AS (
SELECT 
      d2.v2__user_id AS "v2__user_id", 
      d2.v0__user_id AS "v0__user_id", 
      v4.user_id AS "v4__user_id"
FROM vlp_v3_path AS v3
JOIN optional_d2 AS d2 ON v3.start_id = d2.v2__user_id
JOIN test_integration.users_test AS v4 ON v3.end_id = v4.user_id
WHERE v4.user_id <> d2.v0__user_id
)
SELECT 
      count(*) AS "k"
FROM with_w1 AS w1
LEFT JOIN optional_o3 AS o3 ON w1.v2__user_id = o3.v2__user_id AND w1.v0__user_id = o3.v0__user_id

-- column k: Value [("k", "k")]