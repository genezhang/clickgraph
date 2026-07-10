WITH vlp_multi_type_a_b AS (
SELECT '' AS end_type, CAST(NULL AS BIGINT) AS end_id, CAST(NULL AS BIGINT) AS start_id, '' AS start_type, '{}' AS end_properties, '{}' AS start_properties, 0 AS hop_count, CAST(array() AS ARRAY<STRING>) AS path_relationships, CAST(array() AS ARRAY<STRING>) AS rel_properties, CAST(array() AS ARRAY<STRING>) AS path_nodes WHERE 0 = 1
)
SELECT 
      element_at(t.path_relationships, 1) AS `type(r)`, 
      count(*) AS `cnt`
FROM vlp_multi_type_a_b AS t
GROUP BY element_at(t.path_relationships, 1)
