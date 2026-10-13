WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(to_variant_object(map('elementId', CAST(concat('User:', CAST(start_node.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(start_node.age AS VARIANT), 'city', CAST(start_node.city AS VARIANT), 'country', CAST(start_node.country AS VARIANT), 'email', CAST(start_node.email_address AS VARIANT), 'is_active', CAST(start_node.is_active AS VARIANT), 'name', CAST(start_node.full_name AS VARIANT), 'registration_date', CAST(start_node.registration_date AS VARIANT), 'user_id', CAST(start_node.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))), to_variant_object(map('elementId', CAST(concat('User:', CAST(end_node.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(end_node.age AS VARIANT), 'city', CAST(end_node.city AS VARIANT), 'country', CAST(end_node.country AS VARIANT), 'email', CAST(end_node.email_address AS VARIANT), 'is_active', CAST(end_node.is_active AS VARIANT), 'name', CAST(end_node.full_name AS VARIANT), 'registration_date', CAST(end_node.registration_date AS VARIANT), 'user_id', CAST(end_node.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))) as path_node_values,
        array(rel.follow_id) as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_node_values, array(to_variant_object(map('elementId', CAST(concat('User:', CAST(end_node.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(end_node.age AS VARIANT), 'city', CAST(end_node.city AS VARIANT), 'country', CAST(end_node.country AS VARIANT), 'email', CAST(end_node.email_address AS VARIANT), 'is_active', CAST(end_node.is_active AS VARIANT), 'name', CAST(end_node.full_name AS VARIANT), 'registration_date', CAST(end_node.registration_date AS VARIANT), 'user_id', CAST(end_node.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))))) as path_node_values,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, rel.follow_id)
)
SELECT 
      v1.hop_count AS `length(p)`, 
      concat(array(to_variant_object(map('elementId', CAST(concat('User:', CAST(v0.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(v0.age AS VARIANT), 'city', CAST(v0.city AS VARIANT), 'country', CAST(v0.country AS VARIANT), 'email', CAST(v0.email_address AS VARIANT), 'is_active', CAST(v0.is_active AS VARIANT), 'name', CAST(v0.full_name AS VARIANT), 'registration_date', CAST(v0.registration_date AS VARIANT), 'user_id', CAST(v0.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))), filter(v1.path_node_values, (__x, __i) -> __i > 0)) AS `nodes(p)`
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
LIMIT 5