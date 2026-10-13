SELECT 
      array(to_variant_object(map('elementId', CAST(concat('TestUser:', CAST(v0.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('TestUser') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(v0.age AS VARIANT), 'name', CAST(v0.name AS VARIANT), 'user_id', CAST(v0.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT))), to_variant_object(map('elementId', CAST(concat('TestUser:', CAST(v2.user_id AS STRING), '-') AS VARIANT), 'labels', CAST(array('TestUser') AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('age', CAST(v2.age AS VARIANT), 'name', CAST(v2.name AS VARIANT), 'user_id', CAST(v2.user_id AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))) AS `path_nodes`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE (v0.name = 'Alice' AND v2.name = 'Bob')

-- column path_nodes: Graph(List(Node)) [("path_nodes", "path_nodes")]