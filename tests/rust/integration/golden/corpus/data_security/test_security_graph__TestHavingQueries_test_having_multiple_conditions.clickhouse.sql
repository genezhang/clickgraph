WITH with_folder_name_item_count_cte_0 AS (SELECT 
      folder.name AS "folder_name", 
      count(t0.fs_id) AS "item_count"
FROM data_security.ds_fs_objects AS folder
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = folder.fs_id AND t0.fs_type = 'File'
GROUP BY folder.name
HAVING (item_count >= 1 AND item_count <= 10)
)
SELECT 
      folder_name_item_count.folder_name AS "folder_name.name", 
      folder_name_item_count.item_count AS "item_count"
FROM with_folder_name_item_count_cte_0 AS folder_name_item_count
ORDER BY folder_name_item_count.item_count DESC
