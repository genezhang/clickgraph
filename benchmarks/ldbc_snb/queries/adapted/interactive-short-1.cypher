





















MATCH (n:Person {id: $personId})-[:IS_LOCATED_IN]->(city:City)
RETURN
    n.firstName AS firstName,
    n.lastName AS lastName,
    n.birthday AS birthday,
    n.locationIP AS locationIP,
    n.browserUsed AS browserUsed,
    city.id AS cityId,
    n.gender AS gender,
    n.creationDate AS creationDate
