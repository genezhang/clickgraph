WITH RECURSIVE vlp_a_a AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.user_id] as path_nodes,
        (
            SELECT arraySlice([__seed_edge.follow_id], 1, 0)
            FROM test_integration.user_follows_test AS __seed_edge
            LIMIT 1
        ) as path_edges,
        start_node.age as start_age,
        start_node.age as end_age,
        start_node.city as start_city,
        start_node.city as end_city,
        start_node.country as start_country,
        start_node.country as end_country,
        start_node.email_address as start_email,
        start_node.email_address as end_email,
        start_node.is_active as start_is_active,
        start_node.is_active as end_is_active,
        start_node.full_name as start_name,
        start_node.full_name as end_name,
        start_node.registration_date as start_registration_date,
        start_node.registration_date as end_registration_date
    FROM test_integration.users_test AS start_node
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.user_id]) as path_nodes,
        arrayConcat(vp.path_edges, [rel.follow_id]) as path_edges,
        vp.start_age as start_age,
        end_node.age as end_age,
        vp.start_city as start_city,
        end_node.city as end_city,
        vp.start_country as start_country,
        end_node.country as end_country,
        vp.start_email as start_email,
        end_node.email_address as end_email,
        vp.start_is_active as start_is_active,
        end_node.is_active as end_is_active,
        vp.start_name as start_name,
        end_node.full_name as end_name,
        vp.start_registration_date as start_registration_date,
        end_node.registration_date as end_registration_date
    FROM vlp_a_a vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, rel.follow_id)
)
SELECT 
      count(*) AS "count(*)"
FROM vlp_a_a AS t
WHERE t.start_id = t.end_id
