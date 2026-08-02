WITH vlp_multi_type_a_b AS (
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, string(r1.follower_id) AS r_from_id, string(r1.followed_id) AS r_to_id, to_json(struct(u2.city, u2.country, u2.email_address, u2.is_active, u2.full_name, u2.registration_date, u2.user_id)) AS end_properties, u2.user_id AS end_user_id, to_json(struct(a_1.city, a_1.country, a_1.email_address, a_1.is_active, a_1.full_name, a_1.registration_date, a_1.user_id)) AS start_properties, a_1.city AS start_city, a_1.country AS start_country, a_1.email_address AS start_email, a_1.is_active AS start_is_active, a_1.full_name AS start_name, a_1.registration_date AS start_registration_date, a_1.user_id AS start_user_id, 1 AS hop_count, array('FOLLOWS') AS path_relationships, array(to_json(struct(r1.follow_date))) AS rel_properties, array(string(a_1.user_id), string(u2.user_id)) AS path_nodes
FROM social.users_bench a_1
INNER JOIN social.user_follows_bench r1 ON a_1.user_id = r1.follower_id
INNER JOIN social.users_bench u2 ON r1.followed_id = u2.user_id
WHERE (u2.user_id IN (1, 2, 3))
)
SELECT 
      t.start_name AS `a.name`
FROM vlp_multi_type_a_b AS t
WHERE t.end_id IN (1, 2, 3)
