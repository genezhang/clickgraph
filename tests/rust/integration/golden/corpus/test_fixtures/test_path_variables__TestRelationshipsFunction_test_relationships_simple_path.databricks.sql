SELECT 
      array(to_variant_object(map('elementId', CAST(concat('TEST_FOLLOWS:', CAST(v1.follower_id AS STRING), '->', CAST(v1.followed_id AS STRING), '-') AS VARIANT), 'startNodeElementId', CAST(concat('TestUser:', CAST(v1.follower_id AS STRING), '-') AS VARIANT), 'endNodeElementId', CAST(concat('TestUser:', CAST(v1.followed_id AS STRING), '-') AS VARIANT), 'type', CAST('TEST_FOLLOWS' AS VARIANT), 'properties', CAST(to_variant_object(map_filter(map('since', CAST(v1.since AS VARIANT)), (__k, __v) -> __v IS NOT NULL)) AS VARIANT)))) AS `path_rels`
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
WHERE (v0.name = 'Alice' AND v2.name = 'Bob')

-- column path_rels: Graph(List(Relationship)) [("path_rels", "path_rels")]