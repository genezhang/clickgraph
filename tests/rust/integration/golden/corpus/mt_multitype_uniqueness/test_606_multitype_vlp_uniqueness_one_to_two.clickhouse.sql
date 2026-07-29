WITH vlp_multi_type_a_b AS (
SELECT 'Member' AS end_type, n2.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, formatRowNoNewline('JSONEachRow', n2.id, n2.name) AS end_properties, formatRowNoNewline('JSONEachRow', a_1.id, a_1.name) AS start_properties, 1 AS hop_count, ['HELPS'] AS path_relationships, ['{}'] AS rel_properties, [toString(a_1.id), toString(n2.id)] AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
UNION ALL
SELECT 'Member' AS end_type, n3.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, formatRowNoNewline('JSONEachRow', n3.id, n3.name) AS end_properties, formatRowNoNewline('JSONEachRow', a_1.id, a_1.name) AS start_properties, 2 AS hop_count, ['HELPS', 'HELPS'] AS path_relationships, ['{}', '{}'] AS rel_properties, [toString(a_1.id), toString(n2.id), toString(n3.id)] AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.helps r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
WHERE NOT ((r1.from_id = r2.from_id) AND (r1.to_id = r2.to_id))
UNION ALL
SELECT 'Member' AS end_type, n3.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, formatRowNoNewline('JSONEachRow', n3.id, n3.name) AS end_properties, formatRowNoNewline('JSONEachRow', a_1.id, a_1.name) AS start_properties, 2 AS hop_count, ['HELPS', 'MENTORS'] AS path_relationships, ['{}', '{}'] AS rel_properties, [toString(a_1.id), toString(n2.id), toString(n3.id)] AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.mentors r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
UNION ALL
SELECT 'Member' AS end_type, n2.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, formatRowNoNewline('JSONEachRow', n2.id, n2.name) AS end_properties, formatRowNoNewline('JSONEachRow', a_1.id, a_1.name) AS start_properties, 1 AS hop_count, ['MENTORS'] AS path_relationships, ['{}'] AS rel_properties, [toString(a_1.id), toString(n2.id)] AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
UNION ALL
SELECT 'Member' AS end_type, n3.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, formatRowNoNewline('JSONEachRow', n3.id, n3.name) AS end_properties, formatRowNoNewline('JSONEachRow', a_1.id, a_1.name) AS start_properties, 2 AS hop_count, ['MENTORS', 'HELPS'] AS path_relationships, ['{}', '{}'] AS rel_properties, [toString(a_1.id), toString(n2.id), toString(n3.id)] AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.helps r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
UNION ALL
SELECT 'Member' AS end_type, n3.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, formatRowNoNewline('JSONEachRow', n3.id, n3.name) AS end_properties, formatRowNoNewline('JSONEachRow', a_1.id, a_1.name) AS start_properties, 2 AS hop_count, ['MENTORS', 'MENTORS'] AS path_relationships, ['{}', '{}'] AS rel_properties, [toString(a_1.id), toString(n2.id), toString(n3.id)] AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.mentors r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
WHERE NOT ((r1.from_id = r2.from_id) AND (r1.to_id = r2.to_id))
)
SELECT 
      t.start_id AS "a.id", 
      t.end_id AS "b.id"
FROM vlp_multi_type_a_b AS t
