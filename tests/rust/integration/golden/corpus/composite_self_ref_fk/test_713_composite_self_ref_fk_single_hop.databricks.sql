SELECT 
      a.name AS `a.name`, 
      b.name AS `b.name`
FROM test_integration.fs_objects_composite AS b
INNER JOIN test_integration.fs_objects_composite AS a ON a.parent_region = b.region AND a.parent_id = b.object_id
LIMIT 5