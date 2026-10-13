WITH v1_both AS (
SELECT 
      e.follow_date AS "follow_date", 
      e.follow_id AS "follow_id", 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.follower_id AS "__cg_start_0", 
      e.followed_id AS "__cg_end_0"
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      e.follow_date AS "follow_date", 
      e.follow_id AS "follow_id", 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.followed_id AS "__cg_start_0", 
      e.follower_id AS "__cg_end_0"
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id
), 
optional_o1 AS (
SELECT 
      v0.user_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id"
FROM test_integration.users_test AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.__cg_end_0 = v2.user_id
), 
v3_both AS (
SELECT 
      e.follow_date AS "follow_date", 
      e.follow_id AS "follow_id", 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.follower_id AS "__cg_start_0", 
      e.followed_id AS "__cg_end_0"
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      e.follow_date AS "follow_date", 
      e.follow_id AS "follow_id", 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.followed_id AS "__cg_start_0", 
      e.follower_id AS "__cg_end_0"
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id
), 
optional_o3 AS (
SELECT 
      v2.user_id AS "v2__user_id", 
      v4.user_id AS "v4__user_id"
FROM test_integration.users_test AS v2
JOIN v3_both AS v3 ON v3.__cg_start_0 = v2.user_id
JOIN test_integration.users_test AS v4 ON v3.__cg_end_0 = v4.user_id
WHERE v2.user_id IS NOT NULL
), 
with_w5 AS (
SELECT 
      v0.user_id AS "__cg_w5_c0"
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
LEFT JOIN optional_o3 AS o3 ON o1.v2__user_id = o3.v2__user_id
), 
with_w6 AS (
SELECT 
      v6.user_id AS "__cg_w6_c0"
FROM test_integration.users_test AS v6
), 
with_w7 AS (
SELECT 
      CAST(w5.__cg_w5_c0, 'Dynamic') AS "__cg_c0"
FROM with_w5 AS w5

UNION ALL
SELECT 
      CAST(w6.__cg_w6_c0, 'Dynamic') AS "__cg_c0"
FROM with_w6 AS w6
)
SELECT 
      any(w7.__cg_c0) AS "id"
FROM with_w7 AS w7
GROUP BY tuple(multiIf(dynamicType(CAST(w7.__cg_c0, 'Dynamic')) = 'None', '', dynamicType(CAST(w7.__cg_c0, 'Dynamic')) = 'Bool', 'b', match(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), '^(U?Int[0-9]|Float|BFloat|Decimal)'), 'n', match(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), '^(Array|Map|Tuple)'), 'c', match(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), '^(String|FixedString|Enum|UUID|IPv|LowCardinality)'), 's', dynamicType(CAST(w7.__cg_c0, 'Dynamic')) = 'Date32', 'Date', startsWith(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), 'DateTime'), concat('DateTime ', if(extract(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), '''([^'']*)''') = '', timezone(), extract(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), '''([^'']*)'''))), startsWith(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), 'Time'), 'Time', dynamicType(CAST(w7.__cg_c0, 'Dynamic'))), multiIf(match(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), '^(U?Int[0-9]|Float|BFloat|Decimal)') AND toString(w7.__cg_c0) = '-0', '0', startsWith(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), 'FixedString'), rightPad(toString(w7.__cg_c0), toUInt64OrZero(extract(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), '[0-9]+')), '\0'), startsWith(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), 'DateTime'), concat(replaceRegexpOne(replaceRegexpOne(toString(w7.__cg_c0), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), ' ', ifNull(toString(toUnixTimestamp64Micro(accurateCastOrNull(CAST(w7.__cg_c0, 'Dynamic'), 'DateTime64(6, ''UTC'')'))), '')), startsWith(dynamicType(CAST(w7.__cg_c0, 'Dynamic')), 'Time'), replaceRegexpOne(replaceRegexpOne(toString(w7.__cg_c0), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), toString(w7.__cg_c0)))

-- column id: Value [("id", "id")]