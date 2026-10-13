WITH RECURSIVE vlp_v1_nodes AS (
SELECT 
      'User' AS `__cg_label`, 
      e.user_id AS `__cg_id_0`, 
      concat('User', ':', CAST(e.user_id AS STRING)) AS `__cg_key`, 
      to_variant_object(map('elementId', CAST(concat('User:', CAST(e.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(e.age AS VARIANT), 'city', CAST(e.city AS VARIANT), 'country', CAST(e.country AS VARIANT), 'email', CAST(e.email_address AS VARIANT), 'is_active', CAST(e.is_active AS VARIANT), 'name', CAST(e.full_name AS VARIANT), 'registration_date', CAST(e.registration_date AS VARIANT), 'user_id', CAST(e.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))) AS `__cg_value`
FROM test_integration.users_test AS e
), 
v1_rels AS (
SELECT 
      'FOLLOWS' AS `__cg_type`, 
      'User' AS `__cg_from_label`, 
      'User' AS `__cg_to_label`, 
      e.follow_id AS `__cg_rid_0`, 
      e.follower_id AS `__cg_from_0`, 
      e.followed_id AS `__cg_to_0`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`, 
      'User' AS `__cg_start_label`, 
      'User' AS `__cg_end_label`, 
      concat('FOLLOWS:User:User:', CAST(e.follow_id AS STRING)) AS `__cg_key`
FROM test_integration.user_follows_test AS e
), 
vlp_v1_trails AS (
    SELECT start_node.__cg_label AS start_label, start_node.__cg_id_0 AS start_id, start_node.__cg_label AS end_label, start_node.__cg_id_0 AS end_id, CAST(0 AS INT) AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_edges, array(start_node.__cg_key) AS path_nodes, array(start_node.__cg_value) AS path_node_values
    FROM vlp_v1_nodes AS start_node
    UNION ALL
    SELECT vp.start_label AS start_label, vp.start_id AS start_id, end_node.__cg_label AS end_label, end_node.__cg_id_0 AS end_id, CAST(vp.hop_count + 1 AS INT) AS hop_count, concat(vp.path_edges, array(rel.__cg_key)) AS path_edges, concat(vp.path_nodes, array(end_node.__cg_key)) AS path_nodes, concat(vp.path_node_values, array(end_node.__cg_value)) AS path_node_values
    FROM vlp_v1_trails AS vp
    JOIN v1_rels AS rel ON rel.__cg_start_label = vp.end_label AND rel.__cg_start_0 = vp.end_id
    JOIN vlp_v1_nodes AS end_node ON end_node.__cg_label = rel.__cg_end_label AND end_node.__cg_id_0 = rel.__cg_end_0
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, rel.__cg_key)
), 
vlp_v1_path AS (
    SELECT start_label, start_id, end_label, end_id, hop_count, path_edges, path_nodes, path_node_values
    FROM vlp_v1_trails
    WHERE hop_count >= 1
)
SELECT 
      v1.hop_count AS `length(p)`, 
      concat(array(to_variant_object(map('elementId', CAST(concat('User:', CAST(v0.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('User') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(v0.age AS VARIANT), 'city', CAST(v0.city AS VARIANT), 'country', CAST(v0.country AS VARIANT), 'email', CAST(v0.email_address AS VARIANT), 'is_active', CAST(v0.is_active AS VARIANT), 'name', CAST(v0.full_name AS VARIANT), 'registration_date', CAST(v0.registration_date AS VARIANT), 'user_id', CAST(v0.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))), filter(v1.path_node_values, (__x, __i) -> __i > 0)) AS `nodes(p)`
FROM vlp_v1_path AS v1
JOIN test_integration.users_test AS v0 ON v1.start_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.end_id = v2.user_id
LIMIT 5
-- column length(p): Value [("length(p)", "length(p)")]
-- column nodes(p): Graph(List(Node)) [("nodes(p)", "nodes(p)")]