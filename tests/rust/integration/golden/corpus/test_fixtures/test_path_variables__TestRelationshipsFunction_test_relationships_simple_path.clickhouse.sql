SELECT 
      [map('elementId', CAST(concat('TEST_FOLLOWS:', toString(v1.follower_id), '->', toString(v1.followed_id), '-'), 'Dynamic'), 'startNodeElementId', CAST(concat('TestUser:', toString(v1.follower_id), '-'), 'Dynamic'), 'endNodeElementId', CAST(concat('TestUser:', toString(v1.followed_id), '-'), 'Dynamic'), 'type', CAST('TEST_FOLLOWS', 'Dynamic'), 'properties', CAST(mapFilter((__k, __v) -> __v IS NOT NULL, map('since', CAST(v1.since, 'Dynamic'))), 'Dynamic'))] AS "path_rels"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE (v0.name = 'Alice' AND v2.name = 'Bob')

-- column path_rels: Graph(List(Relationship)) [("path_rels", "path_rels")]