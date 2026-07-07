WITH vlp_multi_type_a_b AS (
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.follower_id) AS r_from_id, toString(r1.followed_id) AS r_to_id, 1 AS hop_count, ['FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.follow_date)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM test_integration.users_test a_1
INNER JOIN test_integration.user_follows_test r1 ON a_1.user_id = r1.follower_id
INNER JOIN test_integration.users_test u2 ON r1.followed_id = u2.user_id
)
SELECT 
      t.path_relationships[1] AS "type(r)", 
      count(*) AS "cnt"
FROM vlp_multi_type_a_b AS t
GROUP BY t.path_relationships[1]
