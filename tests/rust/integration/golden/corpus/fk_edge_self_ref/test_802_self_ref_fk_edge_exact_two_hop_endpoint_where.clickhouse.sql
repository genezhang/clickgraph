SELECT 
      a.name AS "a.name"
FROM test_integration.fs_objects_single AS b
INNER JOIN test_integration.fs_objects_single AS m1 ON m1.parent_id = b.object_id
INNER JOIN test_integration.fs_objects_single AS a ON a.parent_id = m1.object_id
WHERE (b.name = 'x' AND a.object_id <> b.object_id)
