WITH RECURSIVE with_w1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v0.name AS `p2_v0_name`
FROM test_integration.users AS v0
), 
optional_d2 AS (
SELECT DISTINCT 
      w1.v0__user_id AS `v0__user_id`
FROM with_w1 AS w1
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
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      d2.v0__user_id AS `id`
FROM optional_d2 AS d2))
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
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.follower_id, rel.followed_id))
),
vlp_v1_path AS (
    SELECT * FROM vlp_v1_path_inner WHERE hop_count >= 2
), 
optional_o3 AS (
SELECT 
      d2.v0__user_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`
FROM vlp_v1_path AS v1
JOIN optional_d2 AS d2 ON v1.start_id = d2.v0__user_id
JOIN test_integration.users AS v2 ON v1.end_id = v2.user_id
)
SELECT 
      w1.p2_v0_name AS `a.name`, 
      o3.v2__user_id AS `b_id`
FROM with_w1 AS w1
LEFT JOIN optional_o3 AS o3 ON w1.v0__user_id = o3.v0__user_id
ORDER BY w1.p2_v0_name ASC NULLS LAST, o3.v2__user_id ASC NULLS LAST

-- column a.name: Value [("a.name", "a.name")]
-- column b_id: NodeId { label: "TestUser" } [("b_id", "b_id")]