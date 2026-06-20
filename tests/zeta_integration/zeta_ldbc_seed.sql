-- LDBC SNB mini dataset for the DeltaGraph↔zeta-databricks transport gate.
-- Mirrors tests/spark_smoke/mini_delta_seed.sql (same ids/names/edges) but in
-- Zeta-compatible DDL: a SCHEMA named `ldbc` (not a Delta DATABASE), no
-- `USING DELTA`, plain types. Only the tables the harness queries are seeded.
-- Submitted statement-by-statement via the Databricks Statement Execution API.

CREATE SCHEMA IF NOT EXISTS ldbc;

-- Idempotent: drop first so re-running the harness against a persistent Zeta
-- data dir doesn't hit "table already exists" or double-insert rows.
DROP TABLE IF EXISTS ldbc.Person;
DROP TABLE IF EXISTS ldbc.Person_knows_Person;

CREATE TABLE ldbc.Person (
    creationDate BIGINT, id BIGINT, firstName TEXT, lastName TEXT,
    gender TEXT, birthday BIGINT, locationIP TEXT, browserUsed TEXT
);

INSERT INTO ldbc.Person VALUES
(1262304000000, 1, 'Alice', 'Smith', 'female', 631152000000, '1.1.1.1', 'Chrome'),
(1262390400000, 2, 'Bob', 'Jones', 'male', 662688000000, '2.2.2.2', 'Firefox'),
(1262476800000, 3, 'Carol', 'Williams', 'female', 694224000000, '3.3.3.3', 'Safari'),
(1262563200000, 4, 'Dave', 'Brown', 'male', 725846400000, '4.4.4.4', 'Chrome'),
(1262649600000, 5, 'Eve', 'Davis', 'female', 757382400000, '5.5.5.5', 'Firefox');

CREATE TABLE ldbc.Person_knows_Person (
    creationDate BIGINT, Person1Id BIGINT, Person2Id BIGINT
);

INSERT INTO ldbc.Person_knows_Person VALUES
(1262304000000, 1, 2), (1262304000000, 2, 1),
(1262390400000, 1, 3), (1262390400000, 3, 1),
(1262476800000, 2, 3), (1262476800000, 3, 2),
(1262563200000, 3, 4), (1262563200000, 4, 3),
(1262649600000, 4, 5), (1262649600000, 5, 4);
