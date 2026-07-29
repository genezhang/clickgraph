WITH vlp_multi_type_a_b AS (
SELECT 'Member' AS end_type, n2.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, string(r1.from_id) AS r_from_id, string(r1.to_id) AS r_to_id, to_json(struct(n2.id, n2.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 1 AS hop_count, array('HELPS') AS path_relationships, array('{}') AS rel_properties, array(string(a_1.id), string(n2.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.helps r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
UNION ALL
SELECT 'Member' AS end_type, n2.id AS end_id, a_1.id AS start_id, 'Member' AS start_type, string(r1.from_id) AS r_from_id, string(r1.to_id) AS r_to_id, to_json(struct(n2.id, n2.name)) AS end_properties, to_json(struct(a_1.id, a_1.name)) AS start_properties, 1 AS hop_count, array('MENTORS') AS path_relationships, array('{}') AS rel_properties, array(string(a_1.id), string(n2.id)) AS path_nodes
FROM mt606.members a_1
INNER JOIN mt606.mentors r1 ON a_1.id = r1.from_id
INNER JOIN mt606.members n2 ON r1.to_id = n2.id
)
SELECT 
      t.start_id AS `a.id`, 
      t.end_id AS `b.id`
FROM vlp_multi_type_a_b AS t
