WITH RECURSIVE vlp_u1_u2 AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        ['FOLLOWS'] as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges,
        start_node.age as start_age,
        start_node.city as start_city,
        start_node.country as start_country,
        start_node.email_address as start_email,
        start_node.is_active as start_is_active,
        start_node.full_name as start_name,
        start_node.registration_date as start_registration_date
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE start_node.user_id = 1
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_relationships, ['FOLLOWS']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges,
        vp.start_age as start_age,
        vp.start_city as start_city,
        vp.start_country as start_country,
        vp.start_email as start_email,
        vp.start_is_active as start_is_active,
        vp.start_name as start_name,
        vp.start_registration_date as start_registration_date
    FROM vlp_u1_u2 vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
with_hops_path_count_u1_cte_0 AS (SELECT 
      anyLast(start_name) AS "p2_u1_name", 
      hop_count AS "hops", 
      count(*) AS "path_count"
FROM vlp_u1_u2 AS t
GROUP BY t.start_id, length(path)
)
SELECT 
      hops_path_count_u1.p2_u1_name AS "u1.name", 
      hops_path_count_u1.hops AS "hops", 
      hops_path_count_u1.path_count AS "path_count"
FROM with_hops_path_count_u1_cte_0 AS hops_path_count_u1
ORDER BY hops_path_count_u1.hops ASC
