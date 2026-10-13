WITH RECURSIVE with_w1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v0.name AS `p2_v0_name`
FROM test_integration.users AS v0
WHERE v0.name = 'Alice'
), 
optional_d2 AS (
SELECT DISTINCT 
      w1.v0__user_id AS `v0__user_id`
FROM with_w1 AS w1
), 
vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id) as path_nodes,
        (
            SELECT slice(array(struct(__seed_edge.follower_id, __seed_edge.followed_id)), 1, 0)
            FROM test_integration.follows AS __seed_edge
            LIMIT 1
        ) as path_edges
    FROM test_integration.users AS start_node
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
    FROM vlp_v1_path vp
    JOIN test_integration.follows AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2147483647
      AND NOT array_contains(vp.path_edges, struct(rel.follower_id, rel.followed_id))
), 
optional_o3 AS (
SELECT 
      d2.v0__user_id AS `v0__user_id`
FROM vlp_v1_path AS v1
JOIN optional_d2 AS d2 ON v1.start_id = d2.v0__user_id AND v1.end_id = d2.v0__user_id
)
SELECT 
      w1.p2_v0_name AS `a.name`, 
      count(*) AS `paths`
FROM with_w1 AS w1
LEFT JOIN optional_o3 AS o3 ON w1.v0__user_id = o3.v0__user_id
GROUP BY w1.p2_v0_name
