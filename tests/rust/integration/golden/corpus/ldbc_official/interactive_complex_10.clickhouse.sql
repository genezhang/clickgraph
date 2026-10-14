WITH RECURSIVE v1_both AS (
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
vlp_v1_path_inner AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.id, end_node.id] as path_nodes,
        [tuple(rel.Person1Id, rel.Person2Id)] as path_edges
    FROM ldbc.Person AS start_node
    JOIN v1_both AS rel ON start_node.id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE (start_node.id = 14)
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges
    FROM vlp_v1_path_inner vp
    JOIN v1_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN ldbc.Person AS end_node ON rel.__cg_end_0 = end_node.id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
),
vlp_v1_path AS (
    SELECT * FROM vlp_v1_path_inner WHERE hop_count >= 2
), 
v5_both AS (
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
subquery_s4 AS (
SELECT 
      v2.id AS "v2__id", 
      v0.id AS "v0__id", 
      CAST(count(*) > 0 AS Nullable(Bool)) AS "__value"
FROM ldbc.Person AS v2
JOIN v5_both AS v5 ON v5.__cg_start_0 = v2.id
JOIN ldbc.Person AS v0 ON v5.__cg_end_0 = v0.id
WHERE (v0.id = 14 AND NOT v2.id = v0.id)
GROUP BY v2.id, v0.id
), 
with_w5 AS (
SELECT 
      v0.id AS "v7__id", 
      v4.id AS "v8__id", 
      v4.name AS "p2_v8_name", 
      v2.id AS "v9__id", 
      v2.firstName AS "p2_v9_firstName", 
      v2.gender AS "p2_v9_gender", 
      v2.id AS "p2_v9_id", 
      v2.lastName AS "p2_v9_lastName", 
      v2.birthday AS "v10"
FROM ldbc.Person AS v0
JOIN vlp_v1_path AS v1 ON v1.start_id = v0.id
JOIN ldbc.Person AS v2 ON v1.end_id = v2.id
JOIN ldbc.Person_isLocatedIn_Place AS v3 ON v3.PersonId = v2.id
JOIN ldbc.Place AS v4 ON v3.CityId = v4.id
LEFT JOIN subquery_s4 AS s4 ON v2.id = s4.v2__id AND v0.id = s4.v0__id
WHERE (((v4.type = 'City')) AND v0.id = 14 AND NOT v2.id = v0.id AND NOT coalesce(s4.__value, false) AND ((toMonth(fromUnixTimestamp64Milli(v2.birthday)) = 6 AND toDayOfMonth(fromUnixTimestamp64Milli(v2.birthday)) >= 21) OR (toMonth(fromUnixTimestamp64Milli(v2.birthday)) = 6 % 12 + 1 AND toDayOfMonth(fromUnixTimestamp64Milli(v2.birthday)) < 22)))
), 
with_w6 AS (
SELECT DISTINCT 
      w5.v9__id AS "v11__id", 
      w5.p2_v9_firstName AS "p3_v11_firstName", 
      w5.p2_v9_gender AS "p3_v11_gender", 
      w5.p2_v9_id AS "p3_v11_id", 
      w5.p2_v9_lastName AS "p3_v11_lastName", 
      w5.v8__id AS "v12__id", 
      w5.p2_v8_name AS "p3_v12_name", 
      w5.v7__id AS "v13__id"
FROM with_w5 AS w5
), 
optional_o7 AS (
SELECT 
      v14.PersonId AS "v11__id", 
      v15.id AS "v15__id", 
      v15.browserUsed AS "p3_v15_browserUsed", 
      v15.content AS "p3_v15_content", 
      v15.creationDate AS "p3_v15_creationDate", 
      v15.id AS "p3_v15_id", 
      v15.imageFile AS "p3_v15_imageFile", 
      v15.language AS "p3_v15_language", 
      v15.length AS "p3_v15_length", 
      v15.locationIP AS "p3_v15_locationIP"
FROM ldbc.Post_hasCreator_Person AS v14
JOIN ldbc.Post AS v15 ON v14.PostId = v15.id
), 
with_w8 AS (
SELECT 
      w6.v11__id AS "v16__id", 
      w6.p3_v11_firstName AS "p3_v16_firstName", 
      w6.p3_v11_gender AS "p3_v16_gender", 
      w6.p3_v11_id AS "p3_v16_id", 
      w6.p3_v11_lastName AS "p3_v16_lastName", 
      w6.v12__id AS "v17__id", 
      w6.p3_v12_name AS "p3_v17_name", 
      arraySort(groupArrayIf(tuple(o7.v15__id, o7.p3_v15_browserUsed, o7.p3_v15_content, o7.p3_v15_creationDate, o7.p3_v15_id, o7.p3_v15_imageFile, o7.p3_v15_language, o7.p3_v15_length, o7.p3_v15_locationIP), NOT isNull(o7.v15__id))) AS "v18", 
      w6.v13__id AS "v19__id"
FROM with_w6 AS w6
LEFT JOIN optional_o7 AS o7 ON w6.v11__id = o7.v11__id
GROUP BY w6.v11__id, w6.p3_v11_firstName, w6.p3_v11_gender, w6.p3_v11_id, w6.p3_v11_lastName, w6.v12__id, w6.p3_v12_name, w6.v13__id
), 
subquery_d9 AS (
SELECT DISTINCT 
      w8.v19__id AS "v19__id", 
      cityHash64(arraySort(arrayDistinct(arrayMap(__cg_e -> tuple(tupleElement(__cg_e, 1)), w8.v18)))) AS "__cg_members", 
      tupleElement(__cg_member, 1) AS "v20__id"
FROM with_w8 AS w8
ARRAY JOIN arrayDistinct(arrayMap(__cg_e -> tuple(tupleElement(__cg_e, 1)), w8.v18)) AS __cg_member
), 
subquery_s10 AS (
SELECT 
      d9.v19__id AS "v19__id", 
      d9.__cg_members AS "__cg_members", 
      arrayDistinct(arrayMap(t -> t.1, groupArray(tuple(tuple('Post', d9.v20__id))))) AS "__value"
FROM subquery_d9 AS d9
JOIN ldbc.Post_hasTag_Tag AS v21 ON v21.PostId = d9.v20__id
JOIN ldbc.Tag AS v22 ON v21.TagId = v22.id
JOIN ldbc.Person_hasInterest_Tag AS v23 ON v23.PersonId = d9.v19__id AND v23.TagId = v22.id
GROUP BY d9.v19__id, d9.__cg_members
), 
with_w11 AS (
SELECT 
      w8.v16__id AS "v26__id", 
      w8.p3_v16_firstName AS "p3_v26_firstName", 
      w8.p3_v16_gender AS "p3_v26_gender", 
      w8.p3_v16_id AS "p3_v26_id", 
      w8.p3_v16_lastName AS "p3_v26_lastName", 
      w8.v17__id AS "v27__id", 
      w8.p3_v17_name AS "p3_v27_name", 
      length(w8.v18) AS "v28", 
      length(arrayFilter(v25 -> CAST(CASE WHEN tupleElement(v25, 1) IS NULL THEN false WHEN has(arrayIntersect(s10.__value, arrayMap(__cg_e -> tuple('Post', tupleElement(__cg_e, 1)), w8.v18)), tuple('Post', tupleElement(v25, 1))) THEN true ELSE false END AS Nullable(Bool)), w8.v18)) AS "v29"
FROM with_w8 AS w8
LEFT JOIN subquery_s10 AS s10 ON w8.v19__id = s10.v19__id AND cityHash64(arraySort(arrayDistinct(arrayMap(__cg_e -> tuple(tupleElement(__cg_e, 1)), w8.v18)))) = s10.__cg_members
)
SELECT 
      w11.p3_v26_id AS "personId", 
      w11.p3_v26_firstName AS "personFirstName", 
      w11.p3_v26_lastName AS "personLastName", 
      w11.v29 - (w11.v28 - w11.v29) AS "commonInterestScore", 
      w11.p3_v26_gender AS "personGender", 
      w11.p3_v27_name AS "personCityName"
FROM with_w11 AS w11
ORDER BY w11.v29 - (w11.v28 - w11.v29) DESC NULLS FIRST, w11.p3_v26_id ASC
LIMIT 10
-- column personId: Value [("personId", "personId")]
-- column personFirstName: Value [("personFirstName", "personFirstName")]
-- column personLastName: Value [("personLastName", "personLastName")]
-- column commonInterestScore: Value [("commonInterestScore", "commonInterestScore")]
-- column personGender: Value [("personGender", "personGender")]
-- column personCityName: Value [("personCityName", "personCityName")]