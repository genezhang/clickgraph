WITH RECURSIVE vlp_v1_path AS (
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
    WHERE (start_node.user_id = 1)
)
SELECT 
      v2.user_id AS `x.user_id`, 
      v2.full_name AS `x.name`, 
      v2.email_address AS `x.email`, 
      v2.city AS `x.city`, 
      v2.country AS `x.country`
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
WHERE v0.user_id = 1
LIMIT 5
-- column x.user_id: Value [("x.user_id", "x.user_id")]
-- column x.name: Value [("x.name", "x.name")]
-- column x.email: Value [("x.email", "x.email")]
-- column x.city: Value [("x.city", "x.city")]
-- column x.country: Value [("x.country", "x.country")]