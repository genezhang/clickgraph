SELECT 
      v0.name AS `f.name`, 
      v0.path AS `f.path`
FROM data_security.ds_fs_objects AS v0
WHERE (((v0.`fs_type` = 'File')) AND v0.sensitive_data = 1)
ORDER BY v0.name ASC NULLS LAST

-- column f.name: Value [("f.name", "f.name")]
-- column f.path: Value [("f.path", "f.path")]