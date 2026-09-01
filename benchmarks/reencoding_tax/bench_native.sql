-- Native relational representation: Person + Knows(FK) on the SAME Postgres engine.
-- Used by the re-encoding-tax micro-benchmark (Table 6 of the tech report):
-- the native side that Apache AGE's node/edge encoding is compared against.
--
-- Parameters (pass with psql -v):  N = #persons, D = out-degree per person.
-- Reference run:  -v N=100000 -v D=20  ->  100,000 persons, 2,000,000 KNOWS edges.
DROP TABLE IF EXISTS knows;
DROP TABLE IF EXISTS person;

CREATE TABLE person (
  id      bigint PRIMARY KEY,
  name    text NOT NULL,
  country int  NOT NULL
);

CREATE TABLE knows (
  src bigint NOT NULL REFERENCES person(id),
  dst bigint NOT NULL REFERENCES person(id)
);

-- N persons
INSERT INTO person
SELECT g, 'p'||g, (g % 50)
FROM generate_series(1, :N) AS g;

-- Directed KNOWS edges: each person knows ~D others (deterministic pseudo-random via hashing).
-- Avoids self-loops; both directions inserted to mimic an undirected social graph.
INSERT INTO knows
SELECT s, d FROM (
  SELECT p.id AS s,
         1 + ((p.id * 2654435761 + k * 40503) % :N) AS d
  FROM person p
  CROSS JOIN generate_series(1, :D) AS k
) e
WHERE s <> d;

CREATE INDEX knows_src ON knows(src);
CREATE INDEX knows_dst ON knows(dst);
ANALYZE person;
ANALYZE knows;

SELECT 'persons' AS t, count(*) FROM person
UNION ALL SELECT 'knows', count(*) FROM knows;
