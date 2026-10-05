WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges,
        start_node.age as start_age,
        start_node.city as start_city,
        start_node.country as start_country,
        start_node.email_address as start_email,
        start_node.is_active as start_is_active,
        start_node.full_name as start_name,
        start_node.registration_date as start_registration_date,
        end_node.age as end_age,
        end_node.city as end_city,
        end_node.country as end_country,
        end_node.email_address as end_email,
        end_node.is_active as end_is_active,
        end_node.full_name as end_name,
        end_node.registration_date as end_registration_date
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges,
        vp.start_age as start_age,
        vp.start_city as start_city,
        vp.start_country as start_country,
        vp.start_email as start_email,
        vp.start_is_active as start_is_active,
        vp.start_name as start_name,
        vp.start_registration_date as start_registration_date,
        end_node.age as end_age,
        end_node.city as end_city,
        end_node.country as end_country,
        end_node.email_address as end_email,
        end_node.is_active as end_is_active,
        end_node.full_name as end_name,
        end_node.registration_date as end_registration_date
    FROM vlp_a_b vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
), 
vlp_b_c AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id, end_node.user_id] as path_nodes,
        [rel.follow_id] as path_edges,
        start_node.age as start_age,
        start_node.city as start_city,
        start_node.country as start_country,
        start_node.email_address as start_email,
        start_node.is_active as start_is_active,
        start_node.full_name as start_name,
        start_node.registration_date as start_registration_date,
        end_node.age as end_age,
        end_node.city as end_city,
        end_node.country as end_country,
        end_node.email_address as end_email,
        end_node.is_active as end_is_active,
        end_node.full_name as end_name,
        end_node.registration_date as end_registration_date
    FROM test_integration.users_test AS start_node
    JOIN test_integration.user_follows_test AS rel ON start_node.user_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges,
        vp.start_age as start_age,
        vp.start_city as start_city,
        vp.start_country as start_country,
        vp.start_email as start_email,
        vp.start_is_active as start_is_active,
        vp.start_name as start_name,
        vp.start_registration_date as start_registration_date,
        end_node.age as end_age,
        end_node.city as end_city,
        end_node.country as end_country,
        end_node.email_address as end_email,
        end_node.is_active as end_is_active,
        end_node.full_name as end_name,
        end_node.registration_date as end_registration_date
    FROM vlp_b_c vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
)
SELECT 
      count(*) AS "count(*)"
FROM vlp_a_b AS t
JOIN test_integration.users_test AS h ON 1 = 1
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = h.user_id AND t0.followed_id = t.start_id
INNER JOIN test_integration.user_follows_test AS t1 ON t1.follower_id = t.end_id
INNER JOIN vlp_b_c AS t_ch_0 ON t_ch_0.start_id = t.end_id AND NOT hasAny(t_ch_0.path_edges, t.path_edges)
WHERE t1.follow_id <> t0.follow_id
