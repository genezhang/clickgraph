WITH RECURSIVE with_w1 AS (
SELECT 
      v2.id AS `v3__id`, 
      v0.id AS `v4__id`, 
      v0.id AS `p2_v4_id`, 
      v0.name AS `p2_v4_name`, 
      v0.url AS `p2_v4_url`, 
      v1.id AS `v5__id`, 
      v1.id AS `p2_v5_id`, 
      v1.name AS `p2_v5_name`, 
      v1.url AS `p2_v5_url`
FROM ldbc.Place AS v0
CROSS JOIN ldbc.Place AS v1
CROSS JOIN ldbc.Person AS v2
WHERE (((v0.type = 'Country')) AND ((v1.type = 'Country')) AND v0.name = 'India' AND v1.name = 'China' AND v2.id = 14)
ORDER BY `v3__id` ASC NULLS LAST, `v4__id` ASC NULLS LAST, `v5__id` ASC NULLS LAST
LIMIT 1), 
with_w2 AS (
SELECT 
      w1.v3__id AS `v9__id`, 
      w1.v4__id AS `v10__id`, 
      w1.p2_v4_id AS `p3_v10_id`, 
      w1.p2_v4_name AS `p3_v10_name`, 
      w1.p2_v4_url AS `p3_v10_url`, 
      w1.v5__id AS `v11__id`, 
      w1.p2_v5_id AS `p3_v11_id`, 
      w1.p2_v5_name AS `p3_v11_name`, 
      w1.p2_v5_url AS `p3_v11_url`, 
      array_sort(collect_list(CASE WHEN NOT isnull(v6.id) THEN named_struct('_1', v6.id, '_2', v6.id, '_3', v6.name, '_4', v6.url) END), (__cg_sl, __cg_sr) -> CASE WHEN CASE WHEN CAST(__cg_sl AS VARIANT) IS NULL OR schema_of_variant(CAST(__cg_sl AS VARIANT)) = 'VOID' THEN ':' WHEN schema_of_variant(CAST(__cg_sl AS VARIANT)) = 'BOOLEAN' THEN concat('b:', CAST(CAST(__cg_sl AS VARIANT) AS STRING)) WHEN schema_of_variant(CAST(__cg_sl AS VARIANT)) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(CAST(__cg_sl AS VARIANT) AS BIGINT) AS STRING)) WHEN schema_of_variant(CAST(__cg_sl AS VARIANT)) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(CAST(__cg_sl AS VARIANT), '$', 'DOUBLE') = floor(variant_get(CAST(__cg_sl AS VARIANT), '$', 'DOUBLE')) AND abs(variant_get(CAST(__cg_sl AS VARIANT), '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(CAST(__cg_sl AS VARIANT), '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(CAST(__cg_sl AS VARIANT), '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(CAST(__cg_sl AS VARIANT)) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(CAST(__cg_sl AS VARIANT) AS ARRAY<VARIANT>), __cg_e0 -> CASE WHEN __cg_e0 IS NULL OR schema_of_variant(__cg_e0) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e0) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e0 AS STRING)) WHEN schema_of_variant(__cg_e0) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e0 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e0) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e0, '$', 'DOUBLE') = floor(variant_get(__cg_e0, '$', 'DOUBLE')) AND abs(variant_get(__cg_e0, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e0, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e0, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e0) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(__cg_e0 AS ARRAY<VARIANT>), __cg_e1 -> CASE WHEN __cg_e1 IS NULL OR schema_of_variant(__cg_e1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e1 AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e1, '$', 'DOUBLE') = floor(variant_get(__cg_e1, '$', 'DOUBLE')) AND abs(variant_get(__cg_e1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_e1)) WHEN schema_of_variant(__cg_e1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_e1)) ELSE concat(schema_of_variant(__cg_e1), ':', CAST(__cg_e1 AS STRING)) END))) WHEN schema_of_variant(__cg_e0) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(__cg_e0 AS MAP<STRING, VARIANT>), (__cg_k1, __cg_v1) -> CASE WHEN __cg_v1 IS NULL OR schema_of_variant(__cg_v1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v1 AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v1, '$', 'DOUBLE') = floor(variant_get(__cg_v1, '$', 'DOUBLE')) AND abs(variant_get(__cg_v1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_v1)) WHEN schema_of_variant(__cg_v1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_v1)) ELSE concat(schema_of_variant(__cg_v1), ':', CAST(__cg_v1 AS STRING)) END))) ELSE concat(schema_of_variant(__cg_e0), ':', CAST(__cg_e0 AS STRING)) END))) WHEN schema_of_variant(CAST(__cg_sl AS VARIANT)) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(CAST(__cg_sl AS VARIANT) AS MAP<STRING, VARIANT>), (__cg_k0, __cg_v0) -> CASE WHEN __cg_v0 IS NULL OR schema_of_variant(__cg_v0) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v0) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v0 AS STRING)) WHEN schema_of_variant(__cg_v0) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v0 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v0) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v0, '$', 'DOUBLE') = floor(variant_get(__cg_v0, '$', 'DOUBLE')) AND abs(variant_get(__cg_v0, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v0, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v0, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v0) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(__cg_v0 AS ARRAY<VARIANT>), __cg_e1 -> CASE WHEN __cg_e1 IS NULL OR schema_of_variant(__cg_e1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e1 AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e1, '$', 'DOUBLE') = floor(variant_get(__cg_e1, '$', 'DOUBLE')) AND abs(variant_get(__cg_e1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_e1)) WHEN schema_of_variant(__cg_e1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_e1)) ELSE concat(schema_of_variant(__cg_e1), ':', CAST(__cg_e1 AS STRING)) END))) WHEN schema_of_variant(__cg_v0) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(__cg_v0 AS MAP<STRING, VARIANT>), (__cg_k1, __cg_v1) -> CASE WHEN __cg_v1 IS NULL OR schema_of_variant(__cg_v1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v1 AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v1, '$', 'DOUBLE') = floor(variant_get(__cg_v1, '$', 'DOUBLE')) AND abs(variant_get(__cg_v1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_v1)) WHEN schema_of_variant(__cg_v1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_v1)) ELSE concat(schema_of_variant(__cg_v1), ':', CAST(__cg_v1 AS STRING)) END))) ELSE concat(schema_of_variant(__cg_v0), ':', CAST(__cg_v0 AS STRING)) END))) ELSE concat(schema_of_variant(CAST(__cg_sl AS VARIANT)), ':', CAST(CAST(__cg_sl AS VARIANT) AS STRING)) END < CASE WHEN CAST(__cg_sr AS VARIANT) IS NULL OR schema_of_variant(CAST(__cg_sr AS VARIANT)) = 'VOID' THEN ':' WHEN schema_of_variant(CAST(__cg_sr AS VARIANT)) = 'BOOLEAN' THEN concat('b:', CAST(CAST(__cg_sr AS VARIANT) AS STRING)) WHEN schema_of_variant(CAST(__cg_sr AS VARIANT)) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(CAST(__cg_sr AS VARIANT) AS BIGINT) AS STRING)) WHEN schema_of_variant(CAST(__cg_sr AS VARIANT)) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(CAST(__cg_sr AS VARIANT), '$', 'DOUBLE') = floor(variant_get(CAST(__cg_sr AS VARIANT), '$', 'DOUBLE')) AND abs(variant_get(CAST(__cg_sr AS VARIANT), '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(CAST(__cg_sr AS VARIANT), '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(CAST(__cg_sr AS VARIANT), '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(CAST(__cg_sr AS VARIANT)) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(CAST(__cg_sr AS VARIANT) AS ARRAY<VARIANT>), __cg_e0 -> CASE WHEN __cg_e0 IS NULL OR schema_of_variant(__cg_e0) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e0) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e0 AS STRING)) WHEN schema_of_variant(__cg_e0) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e0 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e0) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e0, '$', 'DOUBLE') = floor(variant_get(__cg_e0, '$', 'DOUBLE')) AND abs(variant_get(__cg_e0, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e0, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e0, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e0) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(__cg_e0 AS ARRAY<VARIANT>), __cg_e1 -> CASE WHEN __cg_e1 IS NULL OR schema_of_variant(__cg_e1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e1 AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e1, '$', 'DOUBLE') = floor(variant_get(__cg_e1, '$', 'DOUBLE')) AND abs(variant_get(__cg_e1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_e1)) WHEN schema_of_variant(__cg_e1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_e1)) ELSE concat(schema_of_variant(__cg_e1), ':', CAST(__cg_e1 AS STRING)) END))) WHEN schema_of_variant(__cg_e0) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(__cg_e0 AS MAP<STRING, VARIANT>), (__cg_k1, __cg_v1) -> CASE WHEN __cg_v1 IS NULL OR schema_of_variant(__cg_v1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v1 AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v1, '$', 'DOUBLE') = floor(variant_get(__cg_v1, '$', 'DOUBLE')) AND abs(variant_get(__cg_v1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_v1)) WHEN schema_of_variant(__cg_v1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_v1)) ELSE concat(schema_of_variant(__cg_v1), ':', CAST(__cg_v1 AS STRING)) END))) ELSE concat(schema_of_variant(__cg_e0), ':', CAST(__cg_e0 AS STRING)) END))) WHEN schema_of_variant(CAST(__cg_sr AS VARIANT)) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(CAST(__cg_sr AS VARIANT) AS MAP<STRING, VARIANT>), (__cg_k0, __cg_v0) -> CASE WHEN __cg_v0 IS NULL OR schema_of_variant(__cg_v0) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v0) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v0 AS STRING)) WHEN schema_of_variant(__cg_v0) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v0 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v0) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v0, '$', 'DOUBLE') = floor(variant_get(__cg_v0, '$', 'DOUBLE')) AND abs(variant_get(__cg_v0, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v0, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v0, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v0) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(__cg_v0 AS ARRAY<VARIANT>), __cg_e1 -> CASE WHEN __cg_e1 IS NULL OR schema_of_variant(__cg_e1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e1 AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e1, '$', 'DOUBLE') = floor(variant_get(__cg_e1, '$', 'DOUBLE')) AND abs(variant_get(__cg_e1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_e1)) WHEN schema_of_variant(__cg_e1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_e1)) ELSE concat(schema_of_variant(__cg_e1), ':', CAST(__cg_e1 AS STRING)) END))) WHEN schema_of_variant(__cg_v0) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(__cg_v0 AS MAP<STRING, VARIANT>), (__cg_k1, __cg_v1) -> CASE WHEN __cg_v1 IS NULL OR schema_of_variant(__cg_v1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v1 AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v1, '$', 'DOUBLE') = floor(variant_get(__cg_v1, '$', 'DOUBLE')) AND abs(variant_get(__cg_v1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_v1)) WHEN schema_of_variant(__cg_v1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_v1)) ELSE concat(schema_of_variant(__cg_v1), ':', CAST(__cg_v1 AS STRING)) END))) ELSE concat(schema_of_variant(__cg_v0), ':', CAST(__cg_v0 AS STRING)) END))) ELSE concat(schema_of_variant(CAST(__cg_sr AS VARIANT)), ':', CAST(CAST(__cg_sr AS VARIANT) AS STRING)) END THEN -1 WHEN CASE WHEN CAST(__cg_sl AS VARIANT) IS NULL OR schema_of_variant(CAST(__cg_sl AS VARIANT)) = 'VOID' THEN ':' WHEN schema_of_variant(CAST(__cg_sl AS VARIANT)) = 'BOOLEAN' THEN concat('b:', CAST(CAST(__cg_sl AS VARIANT) AS STRING)) WHEN schema_of_variant(CAST(__cg_sl AS VARIANT)) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(CAST(__cg_sl AS VARIANT) AS BIGINT) AS STRING)) WHEN schema_of_variant(CAST(__cg_sl AS VARIANT)) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(CAST(__cg_sl AS VARIANT), '$', 'DOUBLE') = floor(variant_get(CAST(__cg_sl AS VARIANT), '$', 'DOUBLE')) AND abs(variant_get(CAST(__cg_sl AS VARIANT), '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(CAST(__cg_sl AS VARIANT), '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(CAST(__cg_sl AS VARIANT), '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(CAST(__cg_sl AS VARIANT)) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(CAST(__cg_sl AS VARIANT) AS ARRAY<VARIANT>), __cg_e0 -> CASE WHEN __cg_e0 IS NULL OR schema_of_variant(__cg_e0) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e0) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e0 AS STRING)) WHEN schema_of_variant(__cg_e0) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e0 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e0) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e0, '$', 'DOUBLE') = floor(variant_get(__cg_e0, '$', 'DOUBLE')) AND abs(variant_get(__cg_e0, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e0, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e0, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e0) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(__cg_e0 AS ARRAY<VARIANT>), __cg_e1 -> CASE WHEN __cg_e1 IS NULL OR schema_of_variant(__cg_e1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e1 AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e1, '$', 'DOUBLE') = floor(variant_get(__cg_e1, '$', 'DOUBLE')) AND abs(variant_get(__cg_e1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_e1)) WHEN schema_of_variant(__cg_e1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_e1)) ELSE concat(schema_of_variant(__cg_e1), ':', CAST(__cg_e1 AS STRING)) END))) WHEN schema_of_variant(__cg_e0) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(__cg_e0 AS MAP<STRING, VARIANT>), (__cg_k1, __cg_v1) -> CASE WHEN __cg_v1 IS NULL OR schema_of_variant(__cg_v1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v1 AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v1, '$', 'DOUBLE') = floor(variant_get(__cg_v1, '$', 'DOUBLE')) AND abs(variant_get(__cg_v1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_v1)) WHEN schema_of_variant(__cg_v1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_v1)) ELSE concat(schema_of_variant(__cg_v1), ':', CAST(__cg_v1 AS STRING)) END))) ELSE concat(schema_of_variant(__cg_e0), ':', CAST(__cg_e0 AS STRING)) END))) WHEN schema_of_variant(CAST(__cg_sl AS VARIANT)) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(CAST(__cg_sl AS VARIANT) AS MAP<STRING, VARIANT>), (__cg_k0, __cg_v0) -> CASE WHEN __cg_v0 IS NULL OR schema_of_variant(__cg_v0) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v0) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v0 AS STRING)) WHEN schema_of_variant(__cg_v0) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v0 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v0) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v0, '$', 'DOUBLE') = floor(variant_get(__cg_v0, '$', 'DOUBLE')) AND abs(variant_get(__cg_v0, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v0, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v0, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v0) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(__cg_v0 AS ARRAY<VARIANT>), __cg_e1 -> CASE WHEN __cg_e1 IS NULL OR schema_of_variant(__cg_e1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e1 AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e1, '$', 'DOUBLE') = floor(variant_get(__cg_e1, '$', 'DOUBLE')) AND abs(variant_get(__cg_e1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_e1)) WHEN schema_of_variant(__cg_e1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_e1)) ELSE concat(schema_of_variant(__cg_e1), ':', CAST(__cg_e1 AS STRING)) END))) WHEN schema_of_variant(__cg_v0) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(__cg_v0 AS MAP<STRING, VARIANT>), (__cg_k1, __cg_v1) -> CASE WHEN __cg_v1 IS NULL OR schema_of_variant(__cg_v1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v1 AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v1, '$', 'DOUBLE') = floor(variant_get(__cg_v1, '$', 'DOUBLE')) AND abs(variant_get(__cg_v1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_v1)) WHEN schema_of_variant(__cg_v1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_v1)) ELSE concat(schema_of_variant(__cg_v1), ':', CAST(__cg_v1 AS STRING)) END))) ELSE concat(schema_of_variant(__cg_v0), ':', CAST(__cg_v0 AS STRING)) END))) ELSE concat(schema_of_variant(CAST(__cg_sl AS VARIANT)), ':', CAST(CAST(__cg_sl AS VARIANT) AS STRING)) END > CASE WHEN CAST(__cg_sr AS VARIANT) IS NULL OR schema_of_variant(CAST(__cg_sr AS VARIANT)) = 'VOID' THEN ':' WHEN schema_of_variant(CAST(__cg_sr AS VARIANT)) = 'BOOLEAN' THEN concat('b:', CAST(CAST(__cg_sr AS VARIANT) AS STRING)) WHEN schema_of_variant(CAST(__cg_sr AS VARIANT)) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(CAST(__cg_sr AS VARIANT) AS BIGINT) AS STRING)) WHEN schema_of_variant(CAST(__cg_sr AS VARIANT)) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(CAST(__cg_sr AS VARIANT), '$', 'DOUBLE') = floor(variant_get(CAST(__cg_sr AS VARIANT), '$', 'DOUBLE')) AND abs(variant_get(CAST(__cg_sr AS VARIANT), '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(CAST(__cg_sr AS VARIANT), '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(CAST(__cg_sr AS VARIANT), '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(CAST(__cg_sr AS VARIANT)) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(CAST(__cg_sr AS VARIANT) AS ARRAY<VARIANT>), __cg_e0 -> CASE WHEN __cg_e0 IS NULL OR schema_of_variant(__cg_e0) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e0) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e0 AS STRING)) WHEN schema_of_variant(__cg_e0) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e0 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e0) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e0, '$', 'DOUBLE') = floor(variant_get(__cg_e0, '$', 'DOUBLE')) AND abs(variant_get(__cg_e0, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e0, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e0, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e0) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(__cg_e0 AS ARRAY<VARIANT>), __cg_e1 -> CASE WHEN __cg_e1 IS NULL OR schema_of_variant(__cg_e1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e1 AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e1, '$', 'DOUBLE') = floor(variant_get(__cg_e1, '$', 'DOUBLE')) AND abs(variant_get(__cg_e1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_e1)) WHEN schema_of_variant(__cg_e1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_e1)) ELSE concat(schema_of_variant(__cg_e1), ':', CAST(__cg_e1 AS STRING)) END))) WHEN schema_of_variant(__cg_e0) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(__cg_e0 AS MAP<STRING, VARIANT>), (__cg_k1, __cg_v1) -> CASE WHEN __cg_v1 IS NULL OR schema_of_variant(__cg_v1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v1 AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v1, '$', 'DOUBLE') = floor(variant_get(__cg_v1, '$', 'DOUBLE')) AND abs(variant_get(__cg_v1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_v1)) WHEN schema_of_variant(__cg_v1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_v1)) ELSE concat(schema_of_variant(__cg_v1), ':', CAST(__cg_v1 AS STRING)) END))) ELSE concat(schema_of_variant(__cg_e0), ':', CAST(__cg_e0 AS STRING)) END))) WHEN schema_of_variant(CAST(__cg_sr AS VARIANT)) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(CAST(__cg_sr AS VARIANT) AS MAP<STRING, VARIANT>), (__cg_k0, __cg_v0) -> CASE WHEN __cg_v0 IS NULL OR schema_of_variant(__cg_v0) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v0) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v0 AS STRING)) WHEN schema_of_variant(__cg_v0) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v0 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v0) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v0, '$', 'DOUBLE') = floor(variant_get(__cg_v0, '$', 'DOUBLE')) AND abs(variant_get(__cg_v0, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v0, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v0, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v0) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(__cg_v0 AS ARRAY<VARIANT>), __cg_e1 -> CASE WHEN __cg_e1 IS NULL OR schema_of_variant(__cg_e1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e1 AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e1, '$', 'DOUBLE') = floor(variant_get(__cg_e1, '$', 'DOUBLE')) AND abs(variant_get(__cg_e1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_e1)) WHEN schema_of_variant(__cg_e1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_e1)) ELSE concat(schema_of_variant(__cg_e1), ':', CAST(__cg_e1 AS STRING)) END))) WHEN schema_of_variant(__cg_v0) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(__cg_v0 AS MAP<STRING, VARIANT>), (__cg_k1, __cg_v1) -> CASE WHEN __cg_v1 IS NULL OR schema_of_variant(__cg_v1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v1 AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v1, '$', 'DOUBLE') = floor(variant_get(__cg_v1, '$', 'DOUBLE')) AND abs(variant_get(__cg_v1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_v1)) WHEN schema_of_variant(__cg_v1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_v1)) ELSE concat(schema_of_variant(__cg_v1), ':', CAST(__cg_v1 AS STRING)) END))) ELSE concat(schema_of_variant(__cg_v0), ':', CAST(__cg_v0 AS STRING)) END))) ELSE concat(schema_of_variant(CAST(__cg_sr AS VARIANT)), ':', CAST(CAST(__cg_sr AS VARIANT) AS STRING)) END THEN 1 ELSE 0 END) AS `v12`
FROM with_w1 AS w1
CROSS JOIN ldbc.Place AS v6
JOIN ldbc.Place_isPartOf_Place AS v7 ON v7.Place1Id = v6.id
JOIN ldbc.Place AS v8 ON v7.Place2Id = v8.id
WHERE (((v6.type = 'City')) AND ((v8.type = 'Country')) AND (v8.id = w1.v4__id OR v8.id = w1.v5__id))
GROUP BY w1.v3__id, w1.v4__id, w1.p2_v4_id, w1.p2_v4_name, w1.p2_v4_url, w1.v5__id, w1.p2_v5_id, w1.p2_v5_name, w1.p2_v5_url
), 
v13_both AS (
SELECT 
      e.Person1Id AS `Person1Id`, 
      e.Person2Id AS `Person2Id`, 
      e.creationDate AS `creationDate`, 
      e.Person1Id AS `__cg_start_0`, 
      e.Person2Id AS `__cg_end_0`
FROM ldbc.Person_knows_Person AS e

UNION ALL
SELECT 
      e.Person1Id AS `Person1Id`, 
      e.Person2Id AS `Person2Id`, 
      e.creationDate AS `creationDate`, 
      e.Person2Id AS `__cg_start_0`, 
      e.Person1Id AS `__cg_end_0`
FROM ldbc.Person_knows_Person AS e
WHERE e.Person1Id <> e.Person2Id
), 
vlp_v13_path AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.id, end_node.id) as path_nodes,
        array(struct(rel.Person1Id, rel.Person2Id)) as path_edges
    FROM ldbc.Person AS start_node
    JOIN v13_both AS rel ON start_node.id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE (start_node.id IN (SELECT DISTINCT 
      w2.v9__id AS `id`
FROM with_w2 AS w2))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.Person1Id, rel.Person2Id))) as path_edges
    FROM vlp_v13_path vp
    JOIN v13_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.Person1Id, rel.Person2Id))
), 
with_w5 AS (
SELECT DISTINCT 
      v14.id AS `v17__id`, 
      v14.firstName AS `p3_v17_firstName`, 
      v14.id AS `p3_v17_id`, 
      v14.lastName AS `p3_v17_lastName`, 
      w2.v10__id AS `v18__id`, 
      w2.p3_v10_id AS `p3_v18_id`, 
      w2.p3_v10_name AS `p3_v18_name`, 
      w2.p3_v10_url AS `p3_v18_url`, 
      w2.v11__id AS `v19__id`, 
      w2.p3_v11_id AS `p3_v19_id`, 
      w2.p3_v11_name AS `p3_v19_name`, 
      w2.p3_v11_url AS `p3_v19_url`
FROM vlp_v13_path AS v13
JOIN with_w2 AS w2 ON v13.start_id = w2.v9__id
JOIN ldbc.Person AS v14 ON v13.end_id = v14.id
JOIN ldbc.Person_isLocatedIn_Place AS v15 ON v15.PersonId = v14.id
JOIN ldbc.Place AS v16 ON v15.CityId = v16.id
WHERE (((v16.type = 'City')) AND (NOT w2.v9__id = v14.id AND NOT CAST(CASE WHEN w2.v12 IS NULL THEN NULL WHEN array_size(w2.v12) = 0 THEN false WHEN v16.id IS NULL THEN NULL ELSE array_size(filter(w2.v12, __cg_in -> (__cg_in)._1 = v16.id)) > 0 END AS BOOLEAN)))
), 
v21_labels AS (
SELECT 
      'Comment' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      e.creationDate AS `p3_v21_creationDate__cg0`, 
      NULL AS `p3_v21_creationDate__cg1`
FROM ldbc.Comment AS e

UNION ALL
SELECT 
      'Post' AS `__cg_label`, 
      e.id AS `__cg_id_0`, 
      NULL AS `p3_v21_creationDate__cg0`, 
      e.creationDate AS `p3_v21_creationDate__cg1`
FROM ldbc.Post AS e
), 
v20_rels AS (
SELECT 
      'HAS_CREATOR' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Person' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.PersonId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.PersonId AS `__cg_to_0`, 
      e.PersonId AS `__cg_start_0`, 
      e.CommentId AS `__cg_end_0`, 
      'Person' AS `__cg_start_label`, 
      'Comment' AS `__cg_end_label`, 
      concat('HAS_CREATOR:Comment:Person:', to_json(named_struct('_1', CAST(e.CommentId AS STRING), '_2', CAST(e.PersonId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_hasCreator_Person AS e

UNION ALL
SELECT 
      'HAS_CREATOR' AS `__cg_type`, 
      'Post' AS `__cg_from_label`, 
      'Person' AS `__cg_to_label`, 
      e.PostId AS `__cg_rid_0`, 
      e.PersonId AS `__cg_rid_1`, 
      e.PostId AS `__cg_from_0`, 
      e.PersonId AS `__cg_to_0`, 
      e.PersonId AS `__cg_start_0`, 
      e.PostId AS `__cg_end_0`, 
      'Person' AS `__cg_start_label`, 
      'Post' AS `__cg_end_label`, 
      concat('HAS_CREATOR:Post:Person:', to_json(named_struct('_1', CAST(e.PostId AS STRING), '_2', CAST(e.PersonId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Post_hasCreator_Person AS e
), 
v22_rels AS (
SELECT 
      'IS_LOCATED_IN' AS `__cg_type`, 
      'Comment' AS `__cg_from_label`, 
      'Country' AS `__cg_to_label`, 
      e.CommentId AS `__cg_rid_0`, 
      e.CountryId AS `__cg_rid_1`, 
      e.CommentId AS `__cg_from_0`, 
      e.CountryId AS `__cg_to_0`, 
      e.CommentId AS `__cg_start_0`, 
      e.CountryId AS `__cg_end_0`, 
      'Comment' AS `__cg_start_label`, 
      'Country' AS `__cg_end_label`, 
      concat('IS_LOCATED_IN:Comment:Country:', to_json(named_struct('_1', CAST(e.CommentId AS STRING), '_2', CAST(e.CountryId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Comment_isLocatedIn_Place AS e

UNION ALL
SELECT 
      'IS_LOCATED_IN' AS `__cg_type`, 
      'Post' AS `__cg_from_label`, 
      'Country' AS `__cg_to_label`, 
      e.PostId AS `__cg_rid_0`, 
      e.CountryId AS `__cg_rid_1`, 
      e.PostId AS `__cg_from_0`, 
      e.CountryId AS `__cg_to_0`, 
      e.PostId AS `__cg_start_0`, 
      e.CountryId AS `__cg_end_0`, 
      'Post' AS `__cg_start_label`, 
      'Country' AS `__cg_end_label`, 
      concat('IS_LOCATED_IN:Post:Country:', to_json(named_struct('_1', CAST(e.PostId AS STRING), '_2', CAST(e.CountryId AS STRING)), map('ignoreNullFields', 'false'))) AS `__cg_key`
FROM ldbc.Post_isLocatedIn_Place AS e
), 
with_w9 AS (
SELECT 
      w5.v17__id AS `v24__id`, 
      w5.p3_v17_firstName AS `p3_v24_firstName`, 
      w5.p3_v17_id AS `p3_v24_id`, 
      w5.p3_v17_lastName AS `p3_v24_lastName`, 
      CASE WHEN v23.id = w5.v18__id THEN 1 ELSE 0 END AS `v25`, 
      CASE WHEN v23.id = w5.v19__id THEN 1 ELSE 0 END AS `v26`
FROM with_w5 AS w5
JOIN v20_rels AS v20 ON v20.__cg_start_0 = w5.v17__id
JOIN v21_labels AS v21 ON v20.__cg_end_label = v21.__cg_label AND v20.__cg_end_0 = v21.__cg_id_0
JOIN v22_rels AS v22 ON v22.__cg_start_label = v21.__cg_label AND v22.__cg_start_0 = v21.__cg_id_0
JOIN ldbc.Place AS v23 ON v22.__cg_end_0 = v23.id
WHERE (((v23.type = 'Country')) AND ((1354157106851 > CASE WHEN regexp_replace(regexp_replace(typeof(v21.p3_v21_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v21.p3_v21_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v21.p3_v21_creationDate__cg0, v21.p3_v21_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END AND CASE WHEN regexp_replace(regexp_replace(typeof(v21.p3_v21_creationDate__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(v21.p3_v21_creationDate__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(v21.p3_v21_creationDate__cg0, v21.p3_v21_creationDate__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END >= 1262305305684) AND (v23.id = w5.v18__id OR v23.id = w5.v19__id)))
), 
with_w10 AS (
SELECT 
      w9.v24__id AS `v27__id`, 
      w9.p3_v24_firstName AS `p3_v27_firstName`, 
      w9.p3_v24_id AS `p3_v27_id`, 
      w9.p3_v24_lastName AS `p3_v27_lastName`, 
      coalesce(sum(w9.v25), 0) AS `v28`, 
      coalesce(sum(w9.v26), 0) AS `v29`
FROM with_w9 AS w9
GROUP BY w9.v24__id, w9.p3_v24_firstName, w9.p3_v24_id, w9.p3_v24_lastName
HAVING (coalesce(sum(w9.v25), 0) > 0 AND coalesce(sum(w9.v26), 0) > 0)
)
SELECT 
      w10.p3_v27_id AS `friendId`, 
      w10.p3_v27_firstName AS `friendFirstName`, 
      w10.p3_v27_lastName AS `friendLastName`, 
      w10.v28 AS `xCount`, 
      w10.v29 AS `yCount`, 
      w10.v28 + w10.v29 AS `xyCount`
FROM with_w10 AS w10
ORDER BY w10.v28 + w10.v29 DESC NULLS FIRST, w10.p3_v27_id ASC NULLS LAST
LIMIT 20
-- column friendId: Value [("friendId", "friendId")]
-- column friendFirstName: Value [("friendFirstName", "friendFirstName")]
-- column friendLastName: Value [("friendLastName", "friendLastName")]
-- column xCount: Value [("xCount", "xCount")]
-- column yCount: Value [("yCount", "yCount")]
-- column xyCount: Value [("xyCount", "xyCount")]