WITH vlp_multi_type_a_b AS (
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.from_id) AS r_from_id, toString(r1.to_id) AS r_to_id, 1 AS hop_count, ['FOLLOWS'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'FOLLOWS' AND r1.from_type = 'User' AND r1.to_type = 'User'
UNION ALL
SELECT 'User' AS end_type, u2.user_id AS end_id, a_1.user_id AS start_id, 'User' AS start_type, toString(r1.from_id) AS r_from_id, toString(r1.to_id) AS r_to_id, 1 AS hop_count, ['LIKES'] AS path_relationships, [formatRowNoNewline('JSONEachRow', r1.timestamp, r1.interaction_weight)] AS rel_properties, [toString(a_1.user_id), toString(u2.user_id)] AS path_nodes
FROM brahmand.users_bench a_1
INNER JOIN brahmand.interactions r1 ON a_1.user_id = r1.from_id
INNER JOIN brahmand.users_bench u2 ON r1.to_id = u2.user_id
WHERE r1.interaction_type = 'LIKES' AND r1.from_type = 'User' AND r1.to_type = 'User'
)
SELECT 
      t.path_relationships[1] AS "type(r)", 
      count(*) AS "cnt"
FROM vlp_multi_type_a_b AS t
GROUP BY t.path_relationships[1]
