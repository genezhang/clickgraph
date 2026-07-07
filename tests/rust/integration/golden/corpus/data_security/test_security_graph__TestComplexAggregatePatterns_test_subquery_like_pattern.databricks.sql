WITH pc_g_member_count_0 AS (
SELECT __r0.group_id AS corr_0, COUNT(*) AS result FROM data_security.ds_memberships AS __r0 GROUP BY __r0.group_id
), 
with_g_member_count_cte_0 AS (SELECT 
      g.name AS `p1_g_name`, 
      COALESCE(pc_g_member_count_0.result, 0) AS `member_count`
FROM data_security.ds_groups AS g
LEFT JOIN pc_g_member_count_0 AS pc_g_member_count_0 ON g.group_id = pc_g_member_count_0.corr_0
)
SELECT 
      g_member_count.p1_g_name AS `g.name`, 
      g_member_count.member_count AS `member_count`
FROM with_g_member_count_cte_0 AS g_member_count
