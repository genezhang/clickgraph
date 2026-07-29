SELECT 
      a.name AS "a.name", 
      b.name AS "b.name"
FROM test_integration.fs_objects_single AS a
INNER JOIN test_integration.fs_objects_single AS m1 ON a.parent_id = m1.object_id
INNER JOIN test_integration.fs_objects_single AS m2 ON m1.parent_id = m2.object_id
INNER JOIN test_integration.fs_objects_single AS b ON m2.parent_id = b.object_id
WHERE a.object_id <> b.object_id
