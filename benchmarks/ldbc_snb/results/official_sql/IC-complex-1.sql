-- LDBC Official Query: IC-complex-1
-- Status: PASS
-- Generated: 2026-02-17T19:11:55.686029
-- Database: ldbc_snb

-- Original Cypher Query:
-- MATCH (p:Person {id: $personId}), (friend:Person {firstName: $firstName})
--        WHERE NOT p=friend
--        WITH p, friend
--        MATCH path = shortestPath((p)-[:KNOWS*1..3]-(friend))
--        WITH min(length(path)) AS distance, friend
-- ORDER BY
--     distance ASC,
--     friend.lastName ASC,
--     toInteger(friend.id) ASC
-- LIMIT 20
-- 
-- MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City)
-- OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[:IS_LOCATED_IN]->(uniCity:City)
-- WITH friend, collect(
--     CASE
--         WHEN uni IS NULL THEN null
--         ELSE [uni.name, studyAt.classYear, uniCity.name]
--     END ) AS unis, friendCity, distance
-- 
-- OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(companyCountry:Country)
-- WITH friend, collect(
--     CASE
--         WHEN company IS NULL then null
--         ELSE [company.name, workAt.workFrom, companyCountry.name]
--     END ) AS companies, unis, friendCity, distance
-- 
-- RETURN
--     friend.id AS friendId,
--     friend.lastName AS friendLastName,
--     distance AS distanceFromPerson,
--     friend.birthday AS friendBirthday,
--     friend.creationDate AS friendCreationDate,
--     friend.gender AS friendGender,
--     friend.browserUsed AS friendBrowserUsed,
--     friend.locationIP AS friendLocationIp,
--     friend.email AS friendEmails,
--     friend.speaks AS friendLanguages,
--     friendCity.name AS friendCityName,
--     unis AS friendUniversities,
--     companies AS friendCompanies
-- ORDER BY
--     distanceFromPerson ASC,
--     friendLastName ASC,
--     toInteger(friendId) ASC
-- LIMIT 20

-- Generated ClickHouse SQL:
WITH RECURSIVE with_friend_p_cte_0 AS (SELECT 
      p.birthday AS "p1_p_birthday", 
      p.browserUsed AS "p1_p_browserUsed", 
      p.creationDate AS "p1_p_creationDate", 
      p.firstName AS "p1_p_firstName", 
      p.gender AS "p1_p_gender", 
      p.id AS "p1_p_id", 
      p.lastName AS "p1_p_lastName", 
      p.locationIP AS "p1_p_locationIP", 
      friend.birthday AS "p6_friend_birthday", 
      friend.browserUsed AS "p6_friend_browserUsed", 
      friend.creationDate AS "p6_friend_creationDate", 
      friend.firstName AS "p6_friend_firstName", 
      friend.gender AS "p6_friend_gender", 
      friend.id AS "p6_friend_id", 
      friend.lastName AS "p6_friend_lastName", 
      friend.locationIP AS "p6_friend_locationIP"
FROM ldbc.Person AS p
CROSS JOIN ldbc.Person AS friend
), 
with_distance_friend_cte_1 AS (SELECT 
      min(hop_count) AS "distance", 
      anyLast(friend_p.p6_friend_birthday) AS "p6_friend_birthday", 
      anyLast(friend_p.p6_friend_browserUsed) AS "p6_friend_browserUsed", 
      anyLast(friend_p.p6_friend_creationDate) AS "p6_friend_creationDate", 
      anyLast(friend_p.p6_friend_firstName) AS "p6_friend_firstName", 
      anyLast(friend_p.p6_friend_gender) AS "p6_friend_gender", 
      friend_p.p6_friend_id AS "p6_friend_id", 
      anyLast(friend_p.p6_friend_lastName) AS "p6_friend_lastName", 
      anyLast(friend_p.p6_friend_locationIP) AS "p6_friend_locationIP"
FROM (
WITH RECURSIVE vlp_friend_p_inner AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.Person1Id, rel.Person2Id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_friend_p_inner vp
    JOIN ldbc.Person_knows_Person AS rel ON vp.end_id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
),
vlp_friend_p_to_target AS (
    SELECT * FROM vlp_friend_p_inner WHERE (end_id = $personId) AND hop_count <= 3
),
vlp_friend_p AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY hop_count ASC) as rn
        FROM vlp_friend_p_to_target
    ) WHERE rn = 1
)
SELECT 
      tuple('fixed_path', 'friend', 'p', 't76') AS "path", 
      '{}' AS "_rel_properties", 
      'KNOWS' AS "__rel_type__", 
      'Person' AS "__start_label__", 
      'Person' AS "__end_label__", 
      t.p6_friend_browserUsed AS "browserUsed", 
      t.p6_friend_gender AS "gender", 
      t.p6_friend_birthday AS "birthday", 
      t.p6_friend_id AS "id", 
      t.p6_friend_firstName AS "firstName", 
      t.p6_friend_locationIP AS "locationIP", 
      t.p6_friend_lastName AS "lastName", 
      t.p6_friend_creationDate AS "creationDate"
FROM vlp_friend_p AS t
) AS __union
GROUP BY friend_p.p6_friend_id
ORDER BY distance ASC, friend_p.p6_friend_lastName ASC, toInt64(friend_p.p6_friend_id) ASC
LIMIT 20
), 
with_distance_friend_friendCity_unis_cte_1 AS (SELECT 
      anyLast(distance_friend.p6_friend_birthday) AS "p6_friend_birthday", 
      anyLast(distance_friend.p6_friend_browserUsed) AS "p6_friend_browserUsed", 
      anyLast(distance_friend.p6_friend_creationDate) AS "p6_friend_creationDate", 
      anyLast(distance_friend.p6_friend_firstName) AS "p6_friend_firstName", 
      anyLast(distance_friend.p6_friend_gender) AS "p6_friend_gender", 
      distance_friend.p6_friend_id AS "p6_friend_id", 
      anyLast(distance_friend.p6_friend_lastName) AS "p6_friend_lastName", 
      anyLast(distance_friend.p6_friend_locationIP) AS "p6_friend_locationIP", 
      groupArray(CASE WHEN friend_p.uni IS NULL THEN NULL ELSE [uni.name, studyAt.classYear, uniCity.name] END) AS "unis", 
      friendCity.id AS "p10_friendCity_id", 
      anyLast(friendCity.name) AS "p10_friendCity_name", 
      anyLast(distance_friend.distance) AS "distance"
FROM with_distance_friend_cte_1 AS distance_friend
INNER JOIN ldbc.Person_isLocatedIn_Place AS t77 ON t77.PersonId = friend_p.p6_friend_id
INNER JOIN ldbc.Place AS friendCity ON friendCity.id = t77.CityId
LEFT JOIN ldbc.Person_studyAt_Organisation AS studyAt ON studyAt.PersonId = friend_p.p6_friend_id
LEFT JOIN (SELECT * FROM ldbc.Organisation WHERE (uni.type = 'University')) AS uni ON uni.id = studyAt.UniversityId
LEFT JOIN ldbc.Organisation_isLocatedIn_Place AS t78 ON t78.OrganisationId = uni.id
LEFT JOIN (SELECT * FROM ldbc.Place WHERE (uniCity.type = 'City')) AS uniCity ON uniCity.id = t78.PlaceId
WHERE (((friendCity.type = 'City') AND (uni.type = 'University')) AND (uniCity.type = 'City'))
GROUP BY friend_p.p6_friend_id, friendCity.id
), 
with_companies_distance_friend_friendCity_unis_cte_1 AS (SELECT 
      anyLast(distance_friend_friendCity_unis.p6_friend_birthday) AS "p6_friend_birthday", 
      anyLast(distance_friend_friendCity_unis.p6_friend_browserUsed) AS "p6_friend_browserUsed", 
      anyLast(distance_friend_friendCity_unis.p6_friend_creationDate) AS "p6_friend_creationDate", 
      anyLast(distance_friend_friendCity_unis.p6_friend_firstName) AS "p6_friend_firstName", 
      anyLast(distance_friend_friendCity_unis.p6_friend_gender) AS "p6_friend_gender", 
      distance_friend_friendCity_unis.p6_friend_id AS "p6_friend_id", 
      anyLast(distance_friend_friendCity_unis.p6_friend_lastName) AS "p6_friend_lastName", 
      anyLast(distance_friend_friendCity_unis.p6_friend_locationIP) AS "p6_friend_locationIP", 
      groupArray(CASE WHEN friend_p.company IS NULL THEN NULL ELSE [company.name, workAt.workFrom, companyCountry.name] END) AS "companies", 
      anyLast(distance_friend_friendCity_unis.unis) AS "unis", 
      distance_friend_friendCity_unis.p10_friendCity_id AS "p10_friendCity_id", 
      anyLast(distance_friend_friendCity_unis.p10_friendCity_name) AS "p10_friendCity_name", 
      anyLast(distance_friend_friendCity_unis.distance) AS "distance"
FROM with_distance_friend_friendCity_unis_cte_1 AS distance_friend_friendCity_unis
LEFT JOIN ldbc.Person_workAt_Organisation AS workAt ON workAt.PersonId = friend_p.p10_friendCity_id
LEFT JOIN (SELECT * FROM ldbc.Organisation WHERE (company.type = 'Company')) AS company ON company.id = workAt.CompanyId
LEFT JOIN ldbc.Organisation_isLocatedIn_Place AS t79 ON t79.OrganisationId = company.id
LEFT JOIN (SELECT * FROM ldbc.Place WHERE (companyCountry.type = 'Country')) AS companyCountry ON companyCountry.id = t79.PlaceId
WHERE ((company.type = 'Company') AND (companyCountry.type = 'Country'))
GROUP BY distance_friend_friendCity_unis.p10_friendCity_id
), 
vlp_p_friend AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_p_friend_inner AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.Person1Id, rel.Person2Id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE start_node.id = $personId
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_p_friend_inner vp
    JOIN ldbc.Person_knows_Person AS rel ON vp.end_id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
),
vlp_p_friend AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY end_id ORDER BY hop_count ASC) as rn
        FROM vlp_p_friend_inner
    ) WHERE rn = 1
)
    SELECT * FROM vlp_p_friend
  )
),
vlp_friend_p AS (
  SELECT * FROM (
    WITH RECURSIVE vlp_friend_p_inner AS (
    SELECT 
        start_node.id as start_id,
        end_node.id as end_id,
        1 as hop_count,
        [tuple(rel.Person1Id, rel.Person2Id)] as path_edges,
        ['KNOWS::Person::Person'] as path_relationships,
        [start_node.id, end_node.id] as path_nodes
    FROM ldbc.Person AS start_node
    JOIN ldbc.Person_knows_Person AS rel ON start_node.id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.id as end_id,
        vp.hop_count + 1 as hop_count,
        arrayConcat(vp.path_edges, [tuple(rel.Person1Id, rel.Person2Id)]) as path_edges,
        arrayConcat(vp.path_relationships, ['KNOWS::Person::Person']) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.id]) as path_nodes
    FROM vlp_friend_p_inner vp
    JOIN ldbc.Person_knows_Person AS rel ON vp.end_id = rel.Person1Id
    JOIN ldbc.Person AS end_node ON rel.Person2Id = end_node.id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.Person1Id, rel.Person2Id))
),
vlp_friend_p_to_target AS (
    SELECT * FROM vlp_friend_p_inner WHERE (end_id = $personId) AND hop_count <= 3
),
vlp_friend_p AS (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY hop_count ASC) as rn
        FROM vlp_friend_p_to_target
    ) WHERE rn = 1
)
    SELECT * FROM vlp_friend_p
  )
)
SELECT 
      companies_distance_friend_friendCity_unis.p6_friend_id AS "friendId", 
      companies_distance_friend_friendCity_unis.p6_friend_lastName AS "friendLastName", 
      companies_distance_friend_friendCity_unis.distance AS "distanceFromPerson", 
      companies_distance_friend_friendCity_unis.p6_friend_birthday AS "friendBirthday", 
      companies_distance_friend_friendCity_unis.p6_friend_creationDate AS "friendCreationDate", 
      companies_distance_friend_friendCity_unis.p6_friend_gender AS "friendGender", 
      companies_distance_friend_friendCity_unis.p6_friend_browserUsed AS "friendBrowserUsed", 
      companies_distance_friend_friendCity_unis.p6_friend_locationIP AS "friendLocationIp", 
      companies_distance_friend_friendCity_unis.email AS "friendEmails", 
      companies_distance_friend_friendCity_unis.speaks AS "friendLanguages", 
      companies_distance_friend_friendCity_unis.p10_friendCity_name AS "friendCityName", 
      companies_distance_friend_friendCity_unis.unis AS "friendUniversities", 
      companies_distance_friend_friendCity_unis.companies AS "friendCompanies"
FROM with_companies_distance_friend_friendCity_unis_cte_1 AS companies_distance_friend_friendCity_unis
ORDER BY distanceFromPerson ASC, friendLastName ASC, toInt64(friendId) ASC
LIMIT  20
