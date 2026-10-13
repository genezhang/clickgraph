WITH v1_both AS (
SELECT 
      e.follow_date AS `follow_date`, 
      e.follow_id AS `follow_id`, 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      e.follow_date AS `follow_date`, 
      e.follow_id AS `follow_id`, 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.followed_id AS `__cg_start_0`, 
      e.follower_id AS `__cg_end_0`
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id
), 
optional_o1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`
FROM test_integration.users_test AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.__cg_end_0 = v2.user_id
), 
v3_both AS (
SELECT 
      e.follow_date AS `follow_date`, 
      e.follow_id AS `follow_id`, 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      e.follow_date AS `follow_date`, 
      e.follow_id AS `follow_id`, 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.followed_id AS `__cg_start_0`, 
      e.follower_id AS `__cg_end_0`
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id
), 
optional_o3 AS (
SELECT 
      v2.user_id AS `v2__user_id`, 
      v4.user_id AS `v4__user_id`
FROM test_integration.users_test AS v2
JOIN v3_both AS v3 ON v3.__cg_start_0 = v2.user_id
JOIN test_integration.users_test AS v4 ON v3.__cg_end_0 = v4.user_id
WHERE v2.user_id IS NOT NULL
), 
with_w5 AS (
SELECT 
      v0.user_id AS `__cg_w5_c0`
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
LEFT JOIN optional_o3 AS o3 ON o1.v2__user_id = o3.v2__user_id
), 
with_w6 AS (
SELECT 
      v6.user_id AS `__cg_w6_c0`
FROM test_integration.users_test AS v6
), 
with_w7 AS (
SELECT 
      CAST(w5.__cg_w5_c0 AS VARIANT) AS `__cg_c0`
FROM with_w5 AS w5

UNION ALL
SELECT 
      CAST(w6.__cg_w6_c0 AS VARIANT) AS `__cg_c0`
FROM with_w6 AS w6
)
SELECT 
      any_value(w7.__cg_c0) AS `id`
FROM with_w7 AS w7
GROUP BY CASE WHEN CAST(w7.__cg_c0 AS VARIANT) IS NULL OR schema_of_variant(CAST(w7.__cg_c0 AS VARIANT)) = 'VOID' THEN ':' WHEN schema_of_variant(CAST(w7.__cg_c0 AS VARIANT)) = 'BOOLEAN' THEN concat('b:', CAST(CAST(w7.__cg_c0 AS VARIANT) AS STRING)) WHEN schema_of_variant(CAST(w7.__cg_c0 AS VARIANT)) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(CAST(w7.__cg_c0 AS VARIANT) AS BIGINT) AS STRING)) WHEN schema_of_variant(CAST(w7.__cg_c0 AS VARIANT)) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(CAST(w7.__cg_c0 AS VARIANT), '$', 'DOUBLE') = floor(variant_get(CAST(w7.__cg_c0 AS VARIANT), '$', 'DOUBLE')) AND abs(variant_get(CAST(w7.__cg_c0 AS VARIANT), '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(CAST(w7.__cg_c0 AS VARIANT), '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(CAST(w7.__cg_c0 AS VARIANT), '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(CAST(w7.__cg_c0 AS VARIANT)) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(CAST(w7.__cg_c0 AS VARIANT) AS ARRAY<VARIANT>), __cg_e0 -> CASE WHEN __cg_e0 IS NULL OR schema_of_variant(__cg_e0) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e0) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e0 AS STRING)) WHEN schema_of_variant(__cg_e0) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e0 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e0) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e0, '$', 'DOUBLE') = floor(variant_get(__cg_e0, '$', 'DOUBLE')) AND abs(variant_get(__cg_e0, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e0, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e0, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e0) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(__cg_e0 AS ARRAY<VARIANT>), __cg_e1 -> CASE WHEN __cg_e1 IS NULL OR schema_of_variant(__cg_e1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e1 AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e1, '$', 'DOUBLE') = floor(variant_get(__cg_e1, '$', 'DOUBLE')) AND abs(variant_get(__cg_e1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_e1)) WHEN schema_of_variant(__cg_e1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_e1)) ELSE concat(schema_of_variant(__cg_e1), ':', CAST(__cg_e1 AS STRING)) END))) WHEN schema_of_variant(__cg_e0) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(__cg_e0 AS MAP<STRING, VARIANT>), (__cg_k1, __cg_v1) -> CASE WHEN __cg_v1 IS NULL OR schema_of_variant(__cg_v1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v1 AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v1, '$', 'DOUBLE') = floor(variant_get(__cg_v1, '$', 'DOUBLE')) AND abs(variant_get(__cg_v1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_v1)) WHEN schema_of_variant(__cg_v1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_v1)) ELSE concat(schema_of_variant(__cg_v1), ':', CAST(__cg_v1 AS STRING)) END))) ELSE concat(schema_of_variant(__cg_e0), ':', CAST(__cg_e0 AS STRING)) END))) WHEN schema_of_variant(CAST(w7.__cg_c0 AS VARIANT)) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(CAST(w7.__cg_c0 AS VARIANT) AS MAP<STRING, VARIANT>), (__cg_k0, __cg_v0) -> CASE WHEN __cg_v0 IS NULL OR schema_of_variant(__cg_v0) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v0) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v0 AS STRING)) WHEN schema_of_variant(__cg_v0) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v0 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v0) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v0, '$', 'DOUBLE') = floor(variant_get(__cg_v0, '$', 'DOUBLE')) AND abs(variant_get(__cg_v0, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v0, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v0, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v0) RLIKE '^ARRAY' THEN concat('l:', to_json(transform(CAST(__cg_v0 AS ARRAY<VARIANT>), __cg_e1 -> CASE WHEN __cg_e1 IS NULL OR schema_of_variant(__cg_e1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_e1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_e1 AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_e1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_e1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_e1, '$', 'DOUBLE') = floor(variant_get(__cg_e1, '$', 'DOUBLE')) AND abs(variant_get(__cg_e1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_e1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_e1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_e1)) WHEN schema_of_variant(__cg_e1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_e1)) ELSE concat(schema_of_variant(__cg_e1), ':', CAST(__cg_e1 AS STRING)) END))) WHEN schema_of_variant(__cg_v0) RLIKE '^OBJECT' THEN concat('m:', to_json(transform_values(CAST(__cg_v0 AS MAP<STRING, VARIANT>), (__cg_k1, __cg_v1) -> CASE WHEN __cg_v1 IS NULL OR schema_of_variant(__cg_v1) = 'VOID' THEN ':' WHEN schema_of_variant(__cg_v1) = 'BOOLEAN' THEN concat('b:', CAST(__cg_v1 AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(TINYINT|SMALLINT|INT|BIGINT)$' THEN concat('n:', CAST(CAST(__cg_v1 AS BIGINT) AS STRING)) WHEN schema_of_variant(__cg_v1) RLIKE '^(FLOAT|DOUBLE|DECIMAL)' THEN concat('n:', CASE WHEN variant_get(__cg_v1, '$', 'DOUBLE') = floor(variant_get(__cg_v1, '$', 'DOUBLE')) AND abs(variant_get(__cg_v1, '$', 'DOUBLE')) < 9.2E18 THEN CAST(CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS BIGINT) AS STRING) ELSE CAST(variant_get(__cg_v1, '$', 'DOUBLE') AS STRING) END) WHEN schema_of_variant(__cg_v1) RLIKE '^ARRAY' THEN concat('c:', to_json(__cg_v1)) WHEN schema_of_variant(__cg_v1) RLIKE '^OBJECT' THEN concat('c:', to_json(__cg_v1)) ELSE concat(schema_of_variant(__cg_v1), ':', CAST(__cg_v1 AS STRING)) END))) ELSE concat(schema_of_variant(__cg_v0), ':', CAST(__cg_v0 AS STRING)) END))) ELSE concat(schema_of_variant(CAST(w7.__cg_c0 AS VARIANT)), ':', CAST(CAST(w7.__cg_c0 AS VARIANT) AS STRING)) END

-- column id: Value [("id", "id")]