SELECT 
      v0.name AS `g.name`
FROM data_security.ds_groups AS v0
JOIN data_security.ds_permissions AS v1 ON v1.subject_id = v0.group_id
JOIN data_security.ds_fs_objects AS v2 ON v1.object_id = v2.fs_id
WHERE ((((v1.`subject_type` = 'Group') AND (v1.`object_type` = 'Folder'))) AND ((v2.`fs_type` = 'Folder')) AND v2.name = 'secrets')

-- column g.name: Value [("g.name", "g.name")]