WITH vlp_multi_type_a_b AS (
SELECT 'Member' AS end_type, n2.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n2.id, n2.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 1 AS hop_count, array('HELPS') AS path_relationships, array('{}') AS rel_properties, array(string(a_1.id), string(n2.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
UNION ALL
SELECT 'Member' AS end_type, n3.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n3.id, n3.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 2 AS hop_count, array('HELPS', 'HELPS') AS path_relationships, array('{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.helps r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
UNION ALL
SELECT 'Member' AS end_type, n4.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n4.id, n4.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 3 AS hop_count, array('HELPS', 'HELPS', 'HELPS') AS path_relationships, array('{}', '{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id), string(n4.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.helps r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
INNER JOIN mt606.helps r3 ON n3.id = r3.from_id
INNER JOIN mt606.members n4 ON r3.to_id = n4.id
UNION ALL
SELECT 'Member' AS end_type, n4.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n4.id, n4.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 3 AS hop_count, array('HELPS', 'HELPS', 'MENTORS') AS path_relationships, array('{}', '{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id), string(n4.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.helps r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
INNER JOIN mt606.mentors r3 ON n3.id = r3.from_id
INNER JOIN mt606.members n4 ON r3.to_id = n4.id
UNION ALL
SELECT 'Member' AS end_type, n3.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n3.id, n3.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 2 AS hop_count, array('HELPS', 'MENTORS') AS path_relationships, array('{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.mentors r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
UNION ALL
SELECT 'Member' AS end_type, n4.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n4.id, n4.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 3 AS hop_count, array('HELPS', 'MENTORS', 'HELPS') AS path_relationships, array('{}', '{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id), string(n4.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.mentors r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
INNER JOIN mt606.helps r3 ON n3.id = r3.from_id
INNER JOIN mt606.members n4 ON r3.to_id = n4.id
UNION ALL
SELECT 'Member' AS end_type, n4.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n4.id, n4.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 3 AS hop_count, array('HELPS', 'MENTORS', 'MENTORS') AS path_relationships, array('{}', '{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id), string(n4.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.mentors r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
INNER JOIN mt606.mentors r3 ON n3.id = r3.from_id
INNER JOIN mt606.members n4 ON r3.to_id = n4.id
UNION ALL
SELECT 'Member' AS end_type, n2.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n2.id, n2.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 1 AS hop_count, array('MENTORS') AS path_relationships, array('{}') AS rel_properties, array(string(a_1.id), string(n2.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
UNION ALL
SELECT 'Member' AS end_type, n3.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n3.id, n3.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 2 AS hop_count, array('MENTORS', 'HELPS') AS path_relationships, array('{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.helps r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
UNION ALL
SELECT 'Member' AS end_type, n4.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n4.id, n4.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 3 AS hop_count, array('MENTORS', 'HELPS', 'HELPS') AS path_relationships, array('{}', '{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id), string(n4.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.helps r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
INNER JOIN mt606.helps r3 ON n3.id = r3.from_id
INNER JOIN mt606.members n4 ON r3.to_id = n4.id
UNION ALL
SELECT 'Member' AS end_type, n4.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n4.id, n4.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 3 AS hop_count, array('MENTORS', 'HELPS', 'MENTORS') AS path_relationships, array('{}', '{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id), string(n4.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.helps r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
INNER JOIN mt606.mentors r3 ON n3.id = r3.from_id
INNER JOIN mt606.members n4 ON r3.to_id = n4.id
UNION ALL
SELECT 'Member' AS end_type, n3.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n3.id, n3.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 2 AS hop_count, array('MENTORS', 'MENTORS') AS path_relationships, array('{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.mentors r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
UNION ALL
SELECT 'Member' AS end_type, n4.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n4.id, n4.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 3 AS hop_count, array('MENTORS', 'MENTORS', 'HELPS') AS path_relationships, array('{}', '{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id), string(n4.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.mentors r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
INNER JOIN mt606.helps r3 ON n3.id = r3.from_id
INNER JOIN mt606.members n4 ON r3.to_id = n4.id
UNION ALL
SELECT 'Member' AS end_type, n4.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, to_json(struct(n4.id, n4.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 3 AS hop_count, array('MENTORS', 'MENTORS', 'MENTORS') AS path_relationships, array('{}', '{}', '{}') AS rel_properties, array(string(a_1.id), string(n2.id), string(n3.id), string(n4.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
INNER JOIN mt606.mentors r2 ON n2.id = r2.from_id
INNER JOIN mt606.members n3 ON r2.to_id = n3.id
INNER JOIN mt606.mentors r3 ON n3.id = r3.from_id
INNER JOIN mt606.members n4 ON r3.to_id = n4.id
)
SELECT 
      t.start_id AS `a.id`, 
      t.end_id AS `b.id`
FROM vlp_multi_type_a_b AS t
