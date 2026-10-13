WITH RECURSIVE with_w1 AS (
SELECT 
      v0.id AS "v2__id", 
      v1.id AS "v3__id", 
      v1.birthday AS "p2_v3_birthday", 
      v1.browserUsed AS "p2_v3_browserUsed", 
      v1.creationDate AS "p2_v3_creationDate", 
      v1.email AS "p2_v3_email", 
      v1.gender AS "p2_v3_gender", 
      v1.id AS "p2_v3_id", 
      v1.lastName AS "p2_v3_lastName", 
      v1.locationIP AS "p2_v3_locationIP", 
      v1.speaks AS "p2_v3_speaks"
FROM ldbc.Person AS v0
CROSS JOIN ldbc.Person AS v1
WHERE (v0.id = 14 AND v1.firstName = 'Hossein' AND NOT v0.id = v1.id)
), 
v4_both AS (
SELECT 
      e.Person1Id AS "Person1Id", 
      e.Person2Id AS "Person2Id", 
      e.creationDate AS "creationDate", 
      e.Person1Id AS "__cg_start_0", 
      e.Person2Id AS "__cg_end_0"
FROM ldbc.Person_knows_Person AS e

UNION ALL
SELECT 
      e.Person1Id AS "Person1Id", 
      e.Person2Id AS "Person2Id", 
      e.creationDate AS "creationDate", 
      e.Person2Id AS "__cg_start_0", 
      e.Person1Id AS "__cg_end_0"
FROM ldbc.Person_knows_Person AS e
WHERE e.Person1Id <> e.Person2Id
), 
vlp_v4_bfs AS (
    SELECT DISTINCT start_node.id AS start_id, start_node.id AS node, CAST(0 AS UInt32) AS depth, CAST(1 AS UInt8) AS new
    FROM ldbc.Person AS start_node
    WHERE start_node.id IS NOT NULL AND (start_node.id IN (SELECT DISTINCT 
      w1.v2__id AS "id"
FROM with_w1 AS w1))
    UNION ALL
    SELECT DISTINCT f.start_id AS start_id, end_node.id AS node, CAST(f.depth + 1 AS UInt32) AS depth, CAST(1 AS UInt8) AS new
    FROM vlp_v4_bfs AS f
    JOIN v4_both AS rel ON rel.__cg_start_0 = f.node
    JOIN ldbc.Person AS end_node ON end_node.id = rel.__cg_end_0
    WHERE f.new = 1
      AND f.depth < 3
      AND (f.start_id, end_node.id) NOT IN (SELECT start_id, node FROM vlp_v4_bfs)
      AND f.start_id NOT IN (SELECT start_id FROM vlp_v4_bfs GROUP BY start_id HAVING countIf(node IN (SELECT end_node.id FROM ldbc.Person AS end_node WHERE (end_node.id IN (SELECT DISTINCT 
      w1.v3__id AS "id"
FROM with_w1 AS w1)))) >= (SELECT count(DISTINCT end_node.id) FROM (SELECT end_node.id FROM ldbc.Person AS end_node WHERE (end_node.id IN (SELECT DISTINCT 
      w1.v3__id AS "id"
FROM with_w1 AS w1))) AS end_node))
    UNION ALL
    SELECT start_id, node, depth, CAST(0 AS UInt8) AS new
    FROM vlp_v4_bfs
    WHERE start_id IN (SELECT start_id FROM vlp_v4_bfs WHERE new = 1)
), 
vlp_v4_path AS (
    SELECT start_id, node AS end_id, depth AS hop_count FROM vlp_v4_bfs
    WHERE new = 1 AND depth >= 1
      AND node IN (SELECT end_node.id FROM ldbc.Person AS end_node WHERE (end_node.id IN (SELECT DISTINCT 
      w1.v3__id AS "id"
FROM with_w1 AS w1)))
), 
with_w5 AS (
SELECT 
      minOrNull(v4.hop_count) AS "v6", 
      w1.v3__id AS "v7__id", 
      w1.p2_v3_birthday AS "p2_v7_birthday", 
      w1.p2_v3_browserUsed AS "p2_v7_browserUsed", 
      w1.p2_v3_creationDate AS "p2_v7_creationDate", 
      w1.p2_v3_email AS "p2_v7_email", 
      w1.p2_v3_gender AS "p2_v7_gender", 
      w1.p2_v3_id AS "p2_v7_id", 
      w1.p2_v3_lastName AS "p2_v7_lastName", 
      w1.p2_v3_locationIP AS "p2_v7_locationIP", 
      w1.p2_v3_speaks AS "p2_v7_speaks", 
      minOrNull(v4.hop_count) AS "__o0", 
      w1.p2_v3_lastName AS "__o1", 
      toInt64(w1.p2_v3_id) AS "__o2"
FROM vlp_v4_path AS v4
JOIN with_w1 AS w1 ON v4.start_id = w1.v2__id AND v4.end_id = w1.v3__id
GROUP BY w1.v3__id, w1.p2_v3_birthday, w1.p2_v3_browserUsed, w1.p2_v3_creationDate, w1.p2_v3_email, w1.p2_v3_gender, w1.p2_v3_id, w1.p2_v3_lastName, w1.p2_v3_locationIP, w1.p2_v3_speaks
ORDER BY minOrNull(v4.hop_count) ASC, w1.p2_v3_lastName ASC, toInt64(w1.p2_v3_id) ASC
LIMIT 20), 
with_w6 AS (
SELECT 
      w5.v6 AS "v6", 
      w5.v7__id AS "v7__id", 
      w5.p2_v7_birthday AS "p2_v7_birthday", 
      w5.p2_v7_browserUsed AS "p2_v7_browserUsed", 
      w5.p2_v7_creationDate AS "p2_v7_creationDate", 
      w5.p2_v7_email AS "p2_v7_email", 
      w5.p2_v7_gender AS "p2_v7_gender", 
      w5.p2_v7_id AS "p2_v7_id", 
      w5.p2_v7_lastName AS "p2_v7_lastName", 
      w5.p2_v7_locationIP AS "p2_v7_locationIP", 
      w5.p2_v7_speaks AS "p2_v7_speaks", 
      v9.id AS "v9__id", 
      v9.name AS "p2_v9_name"
FROM with_w5 AS w5
JOIN ldbc.Person_isLocatedIn_Place AS v8 ON v8.PersonId = w5.v7__id
JOIN ldbc.Place AS v9 ON v8.CityId = v9.id
WHERE ((v9.type = 'City'))
), 
optional_d7 AS (
SELECT DISTINCT 
      w6.v7__id AS "v7__id"
FROM with_w6 AS w6
), 
optional_o8 AS (
SELECT 
      d7.v7__id AS "v7__id", 
      v10.PersonId AS "v10__PersonId", 
      v10.UniversityId AS "v10__UniversityId", 
      v10.classYear AS "p3_v10_classYear", 
      v11.id AS "v11__id", 
      v11.name AS "p3_v11_name", 
      v13.id AS "v13__id", 
      v13.name AS "p3_v13_name"
FROM optional_d7 AS d7
JOIN ldbc.Person_studyAt_Organisation AS v10 ON v10.PersonId = d7.v7__id
JOIN ldbc.Organisation AS v11 ON v10.UniversityId = v11.id
JOIN ldbc.Organisation_isLocatedIn_Place AS v12 ON v12.OrganisationId = v11.id
JOIN ldbc.Place AS v13 ON v12.PlaceId = v13.id
WHERE (((v11.type = 'University')) AND ((v13.type = 'City')))
), 
with_w9 AS (
SELECT 
      w6.v7__id AS "v14__id", 
      w6.p2_v7_birthday AS "p3_v14_birthday", 
      w6.p2_v7_browserUsed AS "p3_v14_browserUsed", 
      w6.p2_v7_creationDate AS "p3_v14_creationDate", 
      w6.p2_v7_email AS "p3_v14_email", 
      w6.p2_v7_gender AS "p3_v14_gender", 
      w6.p2_v7_id AS "p3_v14_id", 
      w6.p2_v7_lastName AS "p3_v14_lastName", 
      w6.p2_v7_locationIP AS "p3_v14_locationIP", 
      w6.p2_v7_speaks AS "p3_v14_speaks", 
      coalesce(groupArrayIf(CASE WHEN o8.v11__id IS NULL THEN [o8.p3_v11_name, o8.p3_v10_classYear, o8.p3_v13_name] ELSE [o8.p3_v11_name, o8.p3_v10_classYear, o8.p3_v13_name] END, CASE WHEN o8.v11__id IS NULL THEN false ELSE true END), []) AS "v15", 
      w6.v9__id AS "v16__id", 
      w6.p2_v9_name AS "p3_v16_name", 
      w6.v6 AS "v17"
FROM with_w6 AS w6
LEFT JOIN optional_o8 AS o8 ON w6.v7__id = o8.v7__id
GROUP BY w6.v7__id, w6.p2_v7_birthday, w6.p2_v7_browserUsed, w6.p2_v7_creationDate, w6.p2_v7_email, w6.p2_v7_gender, w6.p2_v7_id, w6.p2_v7_lastName, w6.p2_v7_locationIP, w6.p2_v7_speaks, w6.v9__id, w6.p2_v9_name, w6.v6
), 
optional_d10 AS (
SELECT DISTINCT 
      w9.v14__id AS "v14__id"
FROM with_w9 AS w9
), 
optional_o11 AS (
SELECT 
      d10.v14__id AS "v14__id", 
      v18.PersonId AS "v18__PersonId", 
      v18.CompanyId AS "v18__CompanyId", 
      v18.workFrom AS "p3_v18_workFrom", 
      v19.id AS "v19__id", 
      v19.name AS "p3_v19_name", 
      v21.id AS "v21__id", 
      v21.name AS "p3_v21_name"
FROM optional_d10 AS d10
JOIN ldbc.Person_workAt_Organisation AS v18 ON v18.PersonId = d10.v14__id
JOIN ldbc.Organisation AS v19 ON v18.CompanyId = v19.id
JOIN ldbc.Organisation_isLocatedIn_Place AS v20 ON v20.OrganisationId = v19.id
JOIN ldbc.Place AS v21 ON v20.PlaceId = v21.id
WHERE (((v19.type = 'Company')) AND ((v21.type = 'Country')))
), 
with_w12 AS (
SELECT 
      w9.v14__id AS "v22__id", 
      w9.p3_v14_birthday AS "p3_v22_birthday", 
      w9.p3_v14_browserUsed AS "p3_v22_browserUsed", 
      w9.p3_v14_creationDate AS "p3_v22_creationDate", 
      w9.p3_v14_email AS "p3_v22_email", 
      w9.p3_v14_gender AS "p3_v22_gender", 
      w9.p3_v14_id AS "p3_v22_id", 
      w9.p3_v14_lastName AS "p3_v22_lastName", 
      w9.p3_v14_locationIP AS "p3_v22_locationIP", 
      w9.p3_v14_speaks AS "p3_v22_speaks", 
      coalesce(groupArrayIf(CASE WHEN o11.v19__id IS NULL THEN [o11.p3_v19_name, o11.p3_v18_workFrom, o11.p3_v21_name] ELSE [o11.p3_v19_name, o11.p3_v18_workFrom, o11.p3_v21_name] END, CASE WHEN o11.v19__id IS NULL THEN false ELSE true END), []) AS "v23", 
      any(w9.v15) AS "v24", 
      w9.v16__id AS "v25__id", 
      w9.p3_v16_name AS "p3_v25_name", 
      w9.v17 AS "v26"
FROM with_w9 AS w9
LEFT JOIN optional_o11 AS o11 ON w9.v14__id = o11.v14__id
GROUP BY w9.v14__id, w9.p3_v14_birthday, w9.p3_v14_browserUsed, w9.p3_v14_creationDate, w9.p3_v14_email, w9.p3_v14_gender, w9.p3_v14_id, w9.p3_v14_lastName, w9.p3_v14_locationIP, w9.p3_v14_speaks, tuple(multiIf(dynamicType(CAST(w9.v15, 'Dynamic')) = 'None', '', dynamicType(CAST(w9.v15, 'Dynamic')) = 'Bool', 'b', match(dynamicType(CAST(w9.v15, 'Dynamic')), '^(U?Int[0-9]|Float|BFloat|Decimal)'), 'n', match(dynamicType(CAST(w9.v15, 'Dynamic')), '^(Array|Map|Tuple)'), 'c', match(dynamicType(CAST(w9.v15, 'Dynamic')), '^(String|FixedString|Enum|UUID|IPv|LowCardinality)'), 's', dynamicType(CAST(w9.v15, 'Dynamic')) = 'Date32', 'Date', startsWith(dynamicType(CAST(w9.v15, 'Dynamic')), 'DateTime'), concat('DateTime ', if(extract(dynamicType(CAST(w9.v15, 'Dynamic')), '''([^'']*)''') = '', timezone(), extract(dynamicType(CAST(w9.v15, 'Dynamic')), '''([^'']*)'''))), startsWith(dynamicType(CAST(w9.v15, 'Dynamic')), 'Time'), 'Time', dynamicType(CAST(w9.v15, 'Dynamic'))), multiIf(match(dynamicType(CAST(w9.v15, 'Dynamic')), '^(U?Int[0-9]|Float|BFloat|Decimal)') AND toString(w9.v15) = '-0', '0', startsWith(dynamicType(CAST(w9.v15, 'Dynamic')), 'FixedString'), rightPad(toString(w9.v15), toUInt64OrZero(extract(dynamicType(CAST(w9.v15, 'Dynamic')), '[0-9]+')), '\0'), startsWith(dynamicType(CAST(w9.v15, 'Dynamic')), 'DateTime'), concat(replaceRegexpOne(replaceRegexpOne(toString(w9.v15), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), ' ', ifNull(toString(toUnixTimestamp64Micro(accurateCastOrNull(if(startsWith(dynamicType(CAST(w9.v15, 'Dynamic')), 'DateTime'), CAST(w9.v15, 'Dynamic'), NULL), 'DateTime64(6, ''UTC'')'))), '')), startsWith(dynamicType(CAST(w9.v15, 'Dynamic')), 'Time'), replaceRegexpOne(replaceRegexpOne(toString(w9.v15), '(\\.[0-9]*[1-9])0+$', '\\1'), '\\.0+$', ''), toString(w9.v15))), w9.v16__id, w9.p3_v16_name, w9.v17
)
SELECT 
      w12.p3_v22_id AS "friendId", 
      w12.p3_v22_lastName AS "friendLastName", 
      w12.v26 AS "distanceFromPerson", 
      w12.p3_v22_birthday AS "friendBirthday", 
      w12.p3_v22_creationDate AS "friendCreationDate", 
      w12.p3_v22_gender AS "friendGender", 
      w12.p3_v22_browserUsed AS "friendBrowserUsed", 
      w12.p3_v22_locationIP AS "friendLocationIp", 
      w12.p3_v22_email AS "friendEmails", 
      w12.p3_v22_speaks AS "friendLanguages", 
      w12.p3_v25_name AS "friendCityName", 
      w12.v24 AS "friendUniversities", 
      w12.v23 AS "friendCompanies"
FROM with_w12 AS w12
ORDER BY w12.v26 ASC, w12.p3_v22_lastName ASC, toInt64(w12.p3_v22_id) ASC
LIMIT 20
-- column friendId: Value [("friendId", "friendId")]
-- column friendLastName: Value [("friendLastName", "friendLastName")]
-- column distanceFromPerson: Value [("distanceFromPerson", "distanceFromPerson")]
-- column friendBirthday: Value [("friendBirthday", "friendBirthday")]
-- column friendCreationDate: Value [("friendCreationDate", "friendCreationDate")]
-- column friendGender: Value [("friendGender", "friendGender")]
-- column friendBrowserUsed: Value [("friendBrowserUsed", "friendBrowserUsed")]
-- column friendLocationIp: Value [("friendLocationIp", "friendLocationIp")]
-- column friendEmails: Value [("friendEmails", "friendEmails")]
-- column friendLanguages: Value [("friendLanguages", "friendLanguages")]
-- column friendCityName: Value [("friendCityName", "friendCityName")]
-- column friendUniversities: Value [("friendUniversities", "friendUniversities")]
-- column friendCompanies: Value [("friendCompanies", "friendCompanies")]