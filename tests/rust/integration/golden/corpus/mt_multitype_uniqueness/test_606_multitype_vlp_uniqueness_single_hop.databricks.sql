WITH v1_rels AS (
SELECT 
      'HELPS' AS `__cg_type`, 
      'Member' AS `__cg_from_label`, 
      'Member' AS `__cg_to_label`, 
      e.from_id AS `__cg_rid_0`, 
      e.to_id AS `__cg_rid_1`, 
      e.from_id AS `__cg_from_0`, 
      e.to_id AS `__cg_to_0`, 
      e.from_id AS `__cg_start_0`, 
      e.to_id AS `__cg_end_0`, 
      'Member' AS `__cg_start_label`, 
      'Member' AS `__cg_end_label`, 
      concat('HELPS:Member:Member:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM mt606.helps AS e

UNION ALL
SELECT 
      'MENTORS' AS `__cg_type`, 
      'Member' AS `__cg_from_label`, 
      'Member' AS `__cg_to_label`, 
      e.from_id AS `__cg_rid_0`, 
      e.to_id AS `__cg_rid_1`, 
      e.from_id AS `__cg_from_0`, 
      e.to_id AS `__cg_to_0`, 
      e.from_id AS `__cg_start_0`, 
      e.to_id AS `__cg_end_0`, 
      'Member' AS `__cg_start_label`, 
      'Member' AS `__cg_end_label`, 
      concat('MENTORS:Member:Member:', to_json(named_struct('_1', CAST(e.from_id AS STRING), '_2', CAST(e.to_id AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM mt606.mentors AS e
)
SELECT 
      v0.id AS `a.id`, 
      v2.id AS `b.id`
FROM mt606.members AS v0
JOIN v1_rels AS v1 ON v1.__cg_start_0 = v0.id
JOIN mt606.members AS v2 ON v1.__cg_end_0 = v2.id

-- column a.id: Value [("a.id", "a.id")]
-- column b.id: Value [("b.id", "b.id")]