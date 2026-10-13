WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
        array(to_variant_object(map('elementId', CAST(concat('User:', CAST(start_node.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(start_node.age AS VARIANT), 'city', CAST(start_node.city AS VARIANT), 'country', CAST(start_node.country AS VARIANT), 'email', CAST(start_node.email_address AS VARIANT), 'is_active', CAST(start_node.is_active AS VARIANT), 'name', CAST(start_node.full_name AS VARIANT), 'registration_date', CAST(start_node.registration_date AS VARIANT), 'user_id', CAST(start_node.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))), to_variant_object(map('elementId', CAST(concat('User:', CAST(end_node.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(end_node.age AS VARIANT), 'city', CAST(end_node.city AS VARIANT), 'country', CAST(end_node.country AS VARIANT), 'email', CAST(end_node.email_address AS VARIANT), 'is_active', CAST(end_node.is_active AS VARIANT), 'name', CAST(end_node.full_name AS VARIANT), 'registration_date', CAST(end_node.registration_date AS VARIANT), 'user_id', CAST(end_node.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))) as path_node_values,
        array(named_struct('_1', start_node.user_id, '_2', start_node.age, '_3', start_node.city, '_4', start_node.country, '_5', start_node.email_address, '_6', start_node.is_active, '_7', start_node.full_name, '_8', start_node.registration_date, '_9', start_node.user_id), named_struct('_1', end_node.user_id, '_2', end_node.age, '_3', end_node.city, '_4', end_node.country, '_5', end_node.email_address, '_6', end_node.is_active, '_7', end_node.full_name, '_8', end_node.registration_date, '_9', end_node.user_id)) as path_node_tuples,
        array(rel.follow_id) as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id = 1)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
        concat(vp.path_node_values, array(to_variant_object(map('elementId', CAST(concat('User:', CAST(end_node.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(end_node.age AS VARIANT), 'city', CAST(end_node.city AS VARIANT), 'country', CAST(end_node.country AS VARIANT), 'email', CAST(end_node.email_address AS VARIANT), 'is_active', CAST(end_node.is_active AS VARIANT), 'name', CAST(end_node.full_name AS VARIANT), 'registration_date', CAST(end_node.registration_date AS VARIANT), 'user_id', CAST(end_node.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))))) as path_node_values,
        concat(vp.path_node_tuples, array(named_struct('_1', end_node.user_id, '_2', end_node.age, '_3', end_node.city, '_4', end_node.country, '_5', end_node.email_address, '_6', end_node.is_active, '_7', end_node.full_name, '_8', end_node.registration_date, '_9', end_node.user_id))) as path_node_tuples,
        concat(vp.path_edges, array(rel.follow_id)) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, rel.follow_id)
), 
with_w2 AS (
SELECT 
      v0.user_id AS `v4__user_id`, 
      v0.full_name AS `p2_v4_name`, 
      v2.user_id AS `v5__user_id`, 
      v2.full_name AS `p2_v5_name`, 
      concat(array(named_struct('_1', v0.user_id, '_2', v0.age, '_3', v0.city, '_4', v0.country, '_5', v0.email_address, '_6', v0.is_active, '_7', v0.full_name, '_8', v0.registration_date, '_9', v0.user_id)), filter(v1.path_node_tuples, (__x, __i) -> __i > 0)) AS `v6`, 
      v1.hop_count AS `v7`
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
WHERE (v0.user_id = 1 AND v1.hop_count = 2)
)
SELECT 
      w2.p2_v4_name AS `u1.name`, 
      w2.p2_v5_name AS `u2.name`, 
      array_size(w2.v6) AS `node_count`
FROM with_w2 AS w2

-- column u1.name: Value [("u1.name", "u1.name")]
-- column u2.name: Value [("u2.name", "u2.name")]
-- column node_count: Value [("node_count", "node_count")]