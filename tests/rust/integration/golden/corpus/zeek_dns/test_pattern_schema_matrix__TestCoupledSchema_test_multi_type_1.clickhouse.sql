WITH vlp_multi_type_a_b AS (
SELECT '' AS end_type, CAST(NULL, 'Nullable(Int64)') AS end_id, CAST(NULL, 'Nullable(Int64)') AS start_id, '' AS start_type, '{}' AS end_properties, '{}' AS start_properties, 0 AS hop_count, CAST([] AS Array(String)) AS path_relationships, CAST([] AS Array(String)) AS rel_properties, CAST([] AS Array(String)) AS path_nodes WHERE 0 = 1
)
SELECT 
      t.path_relationships[1] AS "type(r)", 
      count(*) AS "cnt"
FROM vlp_multi_type_a_b AS t
GROUP BY t.path_relationships[1]
