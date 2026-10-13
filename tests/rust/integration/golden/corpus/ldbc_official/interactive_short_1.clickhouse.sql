SELECT 
      v0.firstName AS "firstName", 
      v0.lastName AS "lastName", 
      v0.birthday AS "birthday", 
      v0.locationIP AS "locationIP", 
      v0.browserUsed AS "browserUsed", 
      v2.id AS "cityId", 
      v0.gender AS "gender", 
      v0.creationDate AS "creationDate"
FROM ldbc.Person AS v0
JOIN ldbc.Person_isLocatedIn_Place AS v1 ON v1.PersonId = v0.id
JOIN ldbc.Place AS v2 ON v1.CityId = v2.id
WHERE (((v2.type = 'City')) AND v0.id = 14)
