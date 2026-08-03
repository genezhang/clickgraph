SELECT 
      count(*) AS "count(*)"
FROM test_integration.fs_objects_composite AS t0
WHERE (t0.parent_region = t0.region AND t0.parent_id = t0.object_id)
