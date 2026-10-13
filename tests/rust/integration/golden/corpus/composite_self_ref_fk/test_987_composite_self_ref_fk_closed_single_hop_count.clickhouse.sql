SELECT 
      count(*) AS "count(*)"
FROM test_integration.fs_objects_composite AS v0
JOIN test_integration.fs_objects_composite AS v1 ON v1.region = v0.region AND v1.object_id = v0.object_id AND v1.parent_region = v0.region AND v1.parent_id = v0.object_id
