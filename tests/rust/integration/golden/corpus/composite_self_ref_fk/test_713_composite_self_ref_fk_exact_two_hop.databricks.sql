SELECT 
      a.name AS `a.name`, 
      b.name AS `b.name`
FROM test_integration.fs_objects_composite AS b
INNER JOIN test_integration.fs_objects_composite AS b ON m1.`parent_region, parent_id` = b.`region, object_id`
INNER JOIN test_integration.fs_objects_composite AS m1 ON a.`parent_region, parent_id` = m1.`region, object_id`
WHERE a.region <> b.region
