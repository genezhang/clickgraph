WITH with_folder_name_item_count_cte_0 AS (SELECT 
      folder.name AS `folder_name`, 
      count(coalesce(item.fs_id, item.group_id, item.user_id)) AS `item_count`
FROM data_security.ds_fs_objects AS folder
INNER JOIN data_security.ds_fs_objects AS t0 ON t0.parent_id = folder.fs_id AND t0.fs_type = 'File'
INNER JOIN data_security.ds_fs_objects AS item ON item.fs_id = t0.fs_id
GROUP BY folder.name
HAVING (item_count >= 1 AND item_count <= 10)
)
SELECT `folder_name.name` AS `folder_name.name`, `item_count` AS `item_count` FROM (
SELECT 
      folder_name_item_count.folder_name AS `folder_name.name`, 
      folder_name_item_count.item_count AS `item_count`, 
      folder_name_item_count.item_count AS `__order_col_0`
FROM with_folder_name_item_count_cte_0 AS folder_name_item_count
UNION ALL 
SELECT 
      folder_name_item_count.folder_name AS `folder_name.name`, 
      with_folder_name_item_count_cte_1.item_count AS `item_count`, 
      folder_name_item_count.item_count AS `__order_col_0`
FROM with_folder_name_item_count_cte_0 AS folder_name_item_count
UNION ALL 
SELECT 
      folder_name_item_count.folder_name AS `folder_name.name`, 
      with_folder_name_item_count_cte_2.item_count AS `item_count`, 
      folder_name_item_count.item_count AS `__order_col_0`
FROM with_folder_name_item_count_cte_0 AS folder_name_item_count
) AS __union
ORDER BY __union.`__order_col_0` DESC
