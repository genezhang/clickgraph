SELECT 
      [map('elementId', CAST(concat('TestUser:', toString(v0.user_id), '-'), 'Dynamic'), 'labels', CAST(['TestUser'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(v0.age, 'Dynamic'), 'name', CAST(v0.name, 'Dynamic'), 'user_id', CAST(v0.user_id, 'Dynamic'))), 'Dynamic')), map('elementId', CAST(concat('TestUser:', toString(v2.user_id), '-'), 'Dynamic'), 'labels', CAST(['TestUser'], 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('age', CAST(v2.age, 'Dynamic'), 'name', CAST(v2.name, 'Dynamic'), 'user_id', CAST(v2.user_id, 'Dynamic'))), 'Dynamic'))] AS "path_nodes"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE (v0.name = 'Alice' AND v2.name = 'Bob')

-- column path_nodes: Graph(List(Node)) [("path_nodes", "path_nodes")]