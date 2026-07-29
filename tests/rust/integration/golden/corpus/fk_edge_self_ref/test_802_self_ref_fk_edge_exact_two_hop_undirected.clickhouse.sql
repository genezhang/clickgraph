SELECT 
      a.name AS "a.name", 
      b.name AS "b.name"
FROM test_integration.fs_objects_single AS a
INNER JOIN test_integration.fs_objects_single AS m1 ON a.parent_id = m1.object_id
INNER JOIN test_integration.fs_objects_single AS b ON m1.parent_id = b.object_id
WHERE a.object_id <> b.object_id
UNION ALL 
SELECT 
      a.name AS "a.name", 
      b.name AS "b.name"
FROM test_integration.fs_objects_single AS b
INNER JOIN test_integration.fs_objects_single AS m1 ON b.parent_id = m1.object_id
INNER JOIN test_integration.fs_objects_single AS a ON m1.parent_id = a.object_id
WHERE b.object_id <> a.object_id
