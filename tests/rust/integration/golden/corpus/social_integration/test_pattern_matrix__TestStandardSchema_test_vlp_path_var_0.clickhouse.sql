WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [map('elementId', CAST(concat('User:', toString(start_node.user_id), '-'), 'Dynamic'), 'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(start_node.age, 'Dynamic'), 'city', CAST(start_node.city, 'Dynamic'), 'country', CAST(start_node.country, 'Dynamic'), 'email', CAST(start_node.email_address, 'Dynamic'), 'is_active', CAST(start_node.is_active, 'Dynamic'), 'name', CAST(start_node.full_name, 'Dynamic'), 'registration_date', CAST(start_node.registration_date, 'Dynamic'), 'user_id', CAST(start_node.user_id, 'Dynamic'))), 'Dynamic')), map('elementId', CAST(concat('User:', toString(end_node.user_id), '-'), 'Dynamic'), 'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(end_node.age, 'Dynamic'), 'city', CAST(end_node.city, 'Dynamic'), 'country', CAST(end_node.country, 'Dynamic'), 'email', CAST(end_node.email_address, 'Dynamic'), 'is_active', CAST(end_node.is_active, 'Dynamic'), 'name', CAST(end_node.full_name, 'Dynamic'), 'registration_date', CAST(end_node.registration_date, 'Dynamic'), 'user_id', CAST(end_node.user_id, 'Dynamic'))), 'Dynamic'))] as path_node_values,
        [rel.follow_id] as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_node_values, [map('elementId', CAST(concat('User:', toString(end_node.user_id), '-'), 'Dynamic'), 'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(end_node.age, 'Dynamic'), 'city', CAST(end_node.city, 'Dynamic'), 'country', CAST(end_node.country, 'Dynamic'), 'email', CAST(end_node.email_address, 'Dynamic'), 'is_active', CAST(end_node.is_active, 'Dynamic'), 'name', CAST(end_node.full_name, 'Dynamic'), 'registration_date', CAST(end_node.registration_date, 'Dynamic'), 'user_id', CAST(end_node.user_id, 'Dynamic'))), 'Dynamic'))]) as path_node_values,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, rel.follow_id)
)
SELECT 
      v1.hop_count AS "length(p)", 
      arrayConcat([map('elementId', CAST(concat('User:', toString(v0.user_id), '-'), 'Dynamic'), 'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(v0.age, 'Dynamic'), 'city', CAST(v0.city, 'Dynamic'), 'country', CAST(v0.country, 'Dynamic'), 'email', CAST(v0.email_address, 'Dynamic'), 'is_active', CAST(v0.is_active, 'Dynamic'), 'name', CAST(v0.full_name, 'Dynamic'), 'registration_date', CAST(v0.registration_date, 'Dynamic'), 'user_id', CAST(v0.user_id, 'Dynamic'))), 'Dynamic'))], arraySlice(v1.path_node_values, 2)) AS "nodes(p)"
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
LIMIT 5