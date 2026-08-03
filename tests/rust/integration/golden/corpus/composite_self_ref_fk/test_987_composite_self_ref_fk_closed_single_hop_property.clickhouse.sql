SELECT 
      a.name AS "a.name"
FROM test_integration.fs_objects_composite AS a
WHERE (a.parent_region = a.region AND a.parent_id = a.object_id)
