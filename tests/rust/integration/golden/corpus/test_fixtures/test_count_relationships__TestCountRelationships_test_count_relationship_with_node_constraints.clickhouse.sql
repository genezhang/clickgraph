WITH vlp_multi_type_u_other AS (
SELECT 'TestUser' AS end_type, n2.user_id AS end_id, u_1.user_id AS start_id, 'TestUser' AS start_type, toString(r1.follower_id) AS r_from_id, toString(r1.followed_id) AS r_to_id, 1 AS hop_count, ['TEST_FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.since)] AS rel_properties, [toString(u_1.user_id), toString(n2.user_id)] AS path_nodes
FROM test_integration.users u_1
INNER JOIN test_integration.follows r1 ON u_1.user_id = r1.follower_id
INNER JOIN test_integration.users n2 ON r1.followed_id = n2.user_id
UNION ALL
SELECT 'TestUser' AS end_type, n2.user_id AS end_id, u_1.user_id AS start_id, 'TestUser' AS start_type, toString(r1.user_id_1) AS r_from_id, toString(r1.user_id_2) AS r_to_id, 1 AS hop_count, ['TEST_FRIENDS_WITH'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.since)] AS rel_properties, [toString(u_1.user_id), toString(n2.user_id)] AS path_nodes
FROM test_integration.users u_1
INNER JOIN test_integration.friendships r1 ON u_1.user_id = r1.user_id_1
INNER JOIN test_integration.users n2 ON r1.user_id_2 = n2.user_id
)
SELECT 
      count(r.follower_id) AS "total"
FROM vlp_multi_type_u_other AS t
