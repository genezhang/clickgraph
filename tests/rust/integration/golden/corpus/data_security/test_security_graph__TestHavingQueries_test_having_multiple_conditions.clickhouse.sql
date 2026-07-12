WITH with_folder_name_item_count_cte_0 AS (SELECT `folder_name` AS "folder_name", count(coalesce(`item.fs_id`, `item.group_id`, `item.user_id`)) AS "item_count" FROM (
SELECT 
      folder.name AS "folder_name",
      folder.name AS "folder.name",
      item.fs_id AS "item.fs_id",
      NULL AS "item.group_id",
      NULL AS "item.user_id"
FROM data_security.ds_fs_objects AS folder
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = folder.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_fs_objects AS item ON item.fs_id = t0.fs_id
UNION ALL 
SELECT 
      folder.name AS "folder_name",
      folder.name AS "folder.name",
      item.fs_id AS "item.fs_id",
      item.group_id AS "item.group_id",
      NULL AS "item.user_id"
FROM data_security.ds_fs_objects AS folder
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = folder.fs_id AND t0.fs_type = 'Group'
INNER JOIN data_security.ds_groups AS item ON item.group_id = t0.fs_id
UNION ALL 
SELECT 
      folder.name AS "folder_name",
      folder.name AS "folder.name",
      item.fs_id AS "item.fs_id",
      NULL AS "item.group_id",
      item.user_id AS "item.user_id"
FROM data_security.ds_fs_objects AS folder
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = folder.fs_id AND t0.fs_type = 'User'
INNER JOIN data_security.ds_users AS item ON item.user_id = t0.fs_id
) AS __union
GROUP BY `folder_name`
HAVING (item_count >= 1 AND item_count <= 10)
)
SELECT 
      folder_name_item_count.folder_name AS "folder_name.name", 
      folder_name_item_count.item_count AS "item_count"
FROM with_folder_name_item_count_cte_0 AS folder_name_item_count
ORDER BY folder_name_item_count.item_count DESC
