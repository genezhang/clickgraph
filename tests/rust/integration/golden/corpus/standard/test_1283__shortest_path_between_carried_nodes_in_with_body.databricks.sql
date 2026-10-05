WITH RECURSIVE with_f_p_cte_0 AS (SELECT 
      p.user_id AS `p1_p_user_id`, 
      f.age AS `p1_f_age`, 
      f.city AS `p1_f_city`, 
      f.country AS `p1_f_country`, 
      f.email_address AS `p1_f_email`, 
      f.is_active AS `p1_f_is_active`, 
      f.full_name AS `p1_f_name`, 
      f.registration_date AS `p1_f_registration_date`, 
      f.user_id AS `p1_f_user_id`
FROM test_integration.users_test AS p
CROSS JOIN test_integration.users_test AS f
WHERE (p.user_id = 1 AND f.user_id IN (2, 7, 15))
), 
vlp_p_f_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        array('FOLLOWS') as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
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
    WHERE start_node.user_id = 1
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        concat(vp.path_relationships, array('FOLLOWS')) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
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
    FROM vlp_p_f_inner vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_nodes, end_node.user_id)
),
vlp_p_f_to_target AS (
    SELECT * FROM vlp_p_f_inner WHERE (end_id IN (2, 7, 15)) AND hop_count <= 3
),
vlp_p_f AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id, end_id ORDER BY hop_count ASC) as rn
        FROM vlp_p_f_to_target
    ) WHERE rn = 1
), 
vlp_f_p_inner AS (
    SELECT 
        start_node.user_id as start_id,
        end_node.user_id as end_id,
        1 as hop_count,
        array('FOLLOWS') as path_relationships,
        array(start_node.user_id, end_node.user_id) as path_nodes,
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
    WHERE start_node.user_id IN (2, 7, 15)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.user_id as end_id,
        vp.hop_count + 1 as hop_count,
        concat(vp.path_relationships, array('FOLLOWS')) as path_relationships,
        concat(vp.path_nodes, array(end_node.user_id)) as path_nodes,
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
    FROM vlp_f_p_inner vp
    JOIN test_integration.user_follows_test AS rel ON vp.end_id = rel.follower_id
    JOIN test_integration.users_test AS end_node ON rel.followed_id = end_node.user_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_nodes, end_node.user_id)
),
vlp_f_p_to_target AS (
    SELECT * FROM vlp_f_p_inner WHERE (end_id = 1) AND hop_count <= 3
),
vlp_f_p AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id, end_id ORDER BY hop_count ASC) as rn
        FROM vlp_f_p_to_target
    ) WHERE rn = 1
), 
with_d_f_cte_1 AS (SELECT min(`hop_count`) AS `d`, `p1_f_user_id` AS `p1_f_user_id` FROM (
SELECT 
      f_p.p1_f_user_id AS `p1_f_user_id`,
      f_p.p1_f_user_id AS `f_p.p1_f_user_id`,
      hop_count AS `hop_count`
FROM vlp_p_f AS t
INNER JOIN with_f_p_cte_0 AS f_p ON string(t.end_id) = string(f_p.p1_f_user_id) AND string(t.start_id) = string(f_p.p1_p_user_id)
UNION ALL 
SELECT 
      f_p.p1_f_user_id AS `p1_f_user_id`,
      f_p.p1_f_user_id AS `f_p.p1_f_user_id`,
      hop_count AS `hop_count`
FROM vlp_f_p AS t
INNER JOIN with_f_p_cte_0 AS f_p ON string(t.end_id) = string(f_p.p1_f_user_id) AND string(t.start_id) = string(f_p.p1_p_user_id)
) AS __union
GROUP BY `p1_f_user_id`
)
SELECT 
      d_f.p1_f_user_id AS `fid`, 
      d_f.d AS `d`
FROM with_d_f_cte_1 AS d_f
