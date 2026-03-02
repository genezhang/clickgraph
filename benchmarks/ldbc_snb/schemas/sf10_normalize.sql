-- sf10 DDL Normalization Script
--
-- sf10 (LDBC Datagen 1.x) uses different column names than sf0.003 (mini_dataset.sql).
-- This script adds ClickHouse ALIAS columns so the YAML schema works unchanged with both.
--
-- Column name divergence:
--   Person_studyAt_Organisation:  sf10 has UniversityId, YAML expects OrganisationId
--   Person_workAt_Organisation:   sf10 has CompanyId,    YAML expects OrganisationId
--   Post_isLocatedIn_Place:       sf10 has CountryId,    YAML expects PlaceId
--   Comment_isLocatedIn_Place:    sf10 has CountryId,    YAML expects PlaceId
--
-- Also creates the Message_isLocatedIn_Place union view (present in sf0.003 DDL but
-- not in sf10's generated DDL).
--
-- Usage:
--   curl 'http://localhost:18123/?user=test_user&password=test_pass' \
--     --data-binary @benchmarks/ldbc_snb/schemas/sf10_normalize.sql

ALTER TABLE ldbc.Person_studyAt_Organisation
  ADD COLUMN IF NOT EXISTS OrganisationId UInt64 ALIAS UniversityId;

ALTER TABLE ldbc.Person_workAt_Organisation
  ADD COLUMN IF NOT EXISTS OrganisationId UInt64 ALIAS CompanyId;

ALTER TABLE ldbc.Post_isLocatedIn_Place
  ADD COLUMN IF NOT EXISTS PlaceId UInt64 ALIAS CountryId;

ALTER TABLE ldbc.Comment_isLocatedIn_Place
  ADD COLUMN IF NOT EXISTS PlaceId UInt64 ALIAS CountryId;

CREATE VIEW IF NOT EXISTS ldbc.Message_isLocatedIn_Place AS
SELECT creationDate, PostId AS MessageId, CountryId AS PlaceId
FROM ldbc.Post_isLocatedIn_Place
UNION ALL
SELECT creationDate, CommentId AS MessageId, CountryId AS PlaceId
FROM ldbc.Comment_isLocatedIn_Place;
