SELECT 
      c.name AS `c.name`
FROM test_integration.fs_objects_single AS p
INNER JOIN test_integration.fs_objects_single AS c ON c.object_id = p.parent_id
WHERE r.parent_id > 0
