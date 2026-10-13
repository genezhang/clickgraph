WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [map('elementId', CAST(concat('User:', toString(start_node.user_id), '-'), 'Dynamic'), 'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(start_node.age, 'Dynamic'), 'city', CAST(start_node.city, 'Dynamic'), 'country', CAST(start_node.country, 'Dynamic'), 'email', CAST(start_node.email_address, 'Dynamic'), 'is_active', CAST(start_node.is_active, 'Dynamic'), 'name', CAST(start_node.full_name, 'Dynamic'), 'registration_date', CAST(start_node.registration_date, 'Dynamic'), 'user_id', CAST(start_node.user_id, 'Dynamic'))), 'Dynamic')), map('elementId', CAST(concat('User:', toString(end_node.user_id), '-'), 'Dynamic'), 'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(end_node.age, 'Dynamic'), 'city', CAST(end_node.city, 'Dynamic'), 'country', CAST(end_node.country, 'Dynamic'), 'email', CAST(end_node.email_address, 'Dynamic'), 'is_active', CAST(end_node.is_active, 'Dynamic'), 'name', CAST(end_node.full_name, 'Dynamic'), 'registration_date', CAST(end_node.registration_date, 'Dynamic'), 'user_id', CAST(end_node.user_id, 'Dynamic'))), 'Dynamic'))] as path_node_values,
        [map('elementId', CAST(concat('FOLLOWS:', toString(rel.follower_id), '->', toString(rel.followed_id), '-'), 'Dynamic'), 'startNodeElementId', CAST(concat('User:', toString(rel.follower_id), '-'), 'Dynamic'), 'endNodeElementId', CAST(concat('User:', toString(rel.followed_id), '-'), 'Dynamic'), 'type', CAST('FOLLOWS', 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('follow_date', CAST(rel.follow_date, 'Dynamic'))), 'Dynamic'))] as path_rel_values,
        [tuple(start_node.user_id, start_node.age, start_node.city, start_node.country, start_node.email_address, start_node.is_active, start_node.full_name, start_node.registration_date, start_node.user_id), tuple(end_node.user_id, end_node.age, end_node.city, end_node.country, end_node.email_address, end_node.is_active, end_node.full_name, end_node.registration_date, end_node.user_id)] as path_node_tuples,
        [tuple(rel.follower_id, rel.followed_id, rel.follow_id, rel.follow_date)] as path_rel_tuples,
        [rel.follow_id] as path_edges
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE (start_node.user_id = 1)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_node_values, [map('elementId', CAST(concat('User:', toString(end_node.user_id), '-'), 'Dynamic'), 'labels', CAST(['User'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(end_node.age, 'Dynamic'), 'city', CAST(end_node.city, 'Dynamic'), 'country', CAST(end_node.country, 'Dynamic'), 'email', CAST(end_node.email_address, 'Dynamic'), 'is_active', CAST(end_node.is_active, 'Dynamic'), 'name', CAST(end_node.full_name, 'Dynamic'), 'registration_date', CAST(end_node.registration_date, 'Dynamic'), 'user_id', CAST(end_node.user_id, 'Dynamic'))), 'Dynamic'))]) as path_node_values,
        arrayConcat(vp.path_rel_values, [map('elementId', CAST(concat('FOLLOWS:', toString(rel.follower_id), '->', toString(rel.followed_id), '-'), 'Dynamic'), 'startNodeElementId', CAST(concat('User:', toString(rel.follower_id), '-'), 'Dynamic'), 'endNodeElementId', CAST(concat('User:', toString(rel.followed_id), '-'), 'Dynamic'), 'type', CAST('FOLLOWS', 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('follow_date', CAST(rel.follow_date, 'Dynamic'))), 'Dynamic'))]) as path_rel_values,
        arrayConcat(vp.path_node_tuples, [tuple(end_node.user_id, end_node.age, end_node.city, end_node.country, end_node.email_address, end_node.is_active, end_node.full_name, end_node.registration_date, end_node.user_id)]) as path_node_tuples,
        arrayConcat(vp.path_rel_tuples, [tuple(rel.follower_id, rel.followed_id, rel.follow_id, rel.follow_date)]) as path_rel_tuples,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
with_w2 AS (
SELECT 
      v0.user_id AS "v4__user_id", 
      v0.full_name AS "p2_v4_name", 
      v2.user_id AS "v5__user_id", 
      v2.full_name AS "p2_v5_name", 
      v1.hop_count AS "v6", 
      arrayConcat([tuple(v0.user_id, v0.age, v0.city, v0.country, v0.email_address, v0.is_active, v0.full_name, v0.registration_date, v0.user_id)], arraySlice(v1.path_node_tuples, 2)) AS "v7", 
      v1.path_rel_tuples AS "v8"
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
WHERE (v0.user_id = 1 AND v1.hop_count = 2)
)
SELECT 
      w2.p2_v4_name AS "u1.name", 
      w2.p2_v5_name AS "u2.name", 
      w2.v6 AS "hops", 
      length(w2.v7) AS "node_count", 
      length(w2.v8) AS "rel_count"
FROM with_w2 AS w2
