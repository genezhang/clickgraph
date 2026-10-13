WITH RECURSIVE subquery_d1 AS (
SELECT DISTINCT 
      v0.user_id AS `v0__user_id`
FROM test_integration.users_test AS v0
), 
vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(rel.follow_id) as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id IN (SELECT DISTINCT 
      d1.v0__user_id AS `id`
FROM subquery_d1 AS d1))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, rel.follow_id)
), 
subquery_s3 AS (
SELECT 
      d1.v0__user_id AS `v0__user_id`, 
      CAST(count(*) > 0 AS BOOLEAN) AS `__value`
FROM vlp_v1_path AS v1
JOIN subquery_d1 AS d1 ON v1.start_id = d1.v0__user_id AND v1.end_id = d1.v0__user_id
GROUP BY d1.v0__user_id
)
SELECT 
      v0.user_id AS `a.user_id`
FROM test_integration.users_test AS v0
LEFT JOIN subquery_s3 AS s3 ON v0.user_id = s3.v0__user_id
WHERE NOT coalesce(s3.__value, false)

-- column a.user_id: Value [("a.user_id", "a.user_id")]