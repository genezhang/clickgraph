SELECT 
      v0.name AS "f.name", 
      v0.path AS "f.path"
FROM data_security.ds_fs_objects AS v0
WHERE ((v0."fs_type" = 'File'))
ORDER BY v0.name ASC

-- column f.name: Value [("f.name", "f.name")]
-- column f.path: Value [("f.path", "f.path")]