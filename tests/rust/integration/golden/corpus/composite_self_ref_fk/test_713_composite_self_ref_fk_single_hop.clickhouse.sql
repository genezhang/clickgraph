SELECT 
      v0.name AS "a.name", 
      v2.name AS "b.name"
FROM test_integration.fs_objects_composite AS v0
JOIN test_integration.fs_objects_composite AS v1 ON v1.region = v0.region AND v1.object_id = v0.object_id
JOIN test_integration.fs_objects_composite AS v2 ON v1.parent_region = v2.region AND v1.parent_id = v2.object_id
LIMIT 5
-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]