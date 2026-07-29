SELECT 
      a.name AS `a.name`, 
      b.name AS `b.name`
FROM test_integration.fs_objects_composite AS a
INNER JOIN test_integration.fs_objects_composite AS b ON b.parent_region = a.region AND b.parent_id = a.object_id
LIMIT 5