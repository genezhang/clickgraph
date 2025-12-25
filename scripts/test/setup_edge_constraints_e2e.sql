-- E2E test data for Edge Constraints feature
-- Tests three schema patterns: Standard, FK-Edge, Denormalized

-- ==========================================
-- 1. Data Lineage (Standard edge table pattern)
-- Constraint: from.timestamp <= to.timestamp
-- ==========================================

CREATE DATABASE IF NOT EXISTS lineage;

DROP TABLE IF EXISTS lineage.data_files;
DROP TABLE IF EXISTS lineage.file_lineage;
DROP TABLE IF EXISTS lineage.analysis_results;
DROP TABLE IF EXISTS lineage.file_analysis;

CREATE TABLE lineage.data_files (
    file_id UInt32,
    file_path String,
    file_size_bytes UInt64,
    created_timestamp DateTime,
    pipeline_stage String,
    file_checksum String
) ENGINE = MergeTree() ORDER BY file_id;

CREATE TABLE lineage.file_lineage (
    source_file_id UInt32,
    target_file_id UInt32,
    copy_operation_type String,
    operation_timestamp DateTime,
    operated_by_user String
) ENGINE = MergeTree() ORDER BY (source_file_id, target_file_id);

CREATE TABLE lineage.analysis_results (
    analysis_id UInt32,
    analysis_name String,
    run_timestamp DateTime,
    execution_status String
) ENGINE = MergeTree() ORDER BY analysis_id;

CREATE TABLE lineage.file_analysis (
    file_id UInt32,
    analysis_id UInt32,
    analysis_timestamp DateTime
) ENGINE = MergeTree() ORDER BY (file_id, analysis_id);

-- Insert test data: file lineage chain
-- raw_1 -> processed_2 -> final_3 (valid: timestamps increase)
-- raw_1 -> bad_4 (invalid: bad_4 timestamp is BEFORE raw_1)
INSERT INTO lineage.data_files VALUES
    (1, '/data/raw/input.csv', 1024, '2025-01-01 10:00:00', 'raw', 'abc123'),
    (2, '/data/processed/clean.csv', 2048, '2025-01-01 11:00:00', 'processed', 'def456'),
    (3, '/data/final/aggregated.csv', 512, '2025-01-01 12:00:00', 'final', 'ghi789'),
    (4, '/data/bad/early.csv', 256, '2025-01-01 09:00:00', 'raw', 'jkl012');

-- Valid lineage (timestamps progress forward)
INSERT INTO lineage.file_lineage VALUES
    (1, 2, 'transform', '2025-01-01 11:00:00', 'etl_user'),
    (2, 3, 'aggregate', '2025-01-01 12:00:00', 'etl_user');

-- Invalid lineage (would violate constraint: 1's timestamp > 4's timestamp)
INSERT INTO lineage.file_lineage VALUES
    (1, 4, 'bad_copy', '2025-01-01 09:30:00', 'bad_user');

INSERT INTO lineage.analysis_results VALUES
    (100, 'Quality Check', '2025-01-01 13:00:00', 'completed');

INSERT INTO lineage.file_analysis VALUES
    (3, 100, '2025-01-01 13:00:00');


-- ==========================================
-- 2. Social Network (Standard node/edge tables)
-- Constraint: from.age > to.age (older follows younger)
-- ==========================================

CREATE DATABASE IF NOT EXISTS social;

DROP TABLE IF EXISTS social.users;
DROP TABLE IF EXISTS social.follows;

CREATE TABLE social.users (
    user_id UInt32,
    username String,
    age UInt8,
    city String
) ENGINE = MergeTree() ORDER BY user_id;

CREATE TABLE social.follows (
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = MergeTree() ORDER BY (follower_id, followed_id);

-- Users with different ages
INSERT INTO social.users VALUES
    (1, 'alice', 30, 'NYC'),
    (2, 'bob', 25, 'SF'),
    (3, 'charlie', 35, 'LA'),
    (4, 'dave', 20, 'NYC');

-- Valid: Alice(30) -> Bob(25) [30 > 25 ✓]
-- Valid: Charlie(35) -> Alice(30) [35 > 30 ✓]
-- Invalid: Bob(25) -> Alice(30) [25 < 30 ✗]
-- Invalid: Dave(20) -> Bob(25) [20 < 25 ✗]
INSERT INTO social.follows VALUES
    (1, 2, '2025-01-01'),
    (3, 1, '2025-01-02'),
    (2, 1, '2025-01-03'),
    (4, 2, '2025-01-04');


-- ==========================================
-- 3. Filesystem (FK-edge pattern)
-- Constraint: from.security_level <= to.security_level
-- (File cannot be in folder with lower security)
-- ==========================================

CREATE DATABASE IF NOT EXISTS filesystem;

DROP TABLE IF EXISTS filesystem.files;
DROP TABLE IF EXISTS filesystem.folders;

CREATE TABLE filesystem.folders (
    folder_id UInt32,
    folder_name String,
    security_level UInt8
) ENGINE = MergeTree() ORDER BY folder_id;

CREATE TABLE filesystem.files (
    file_id UInt32,
    file_name String,
    security_level UInt8,
    parent_folder_id UInt32  -- FK to folders
) ENGINE = MergeTree() ORDER BY file_id;

-- Folders with different security levels
INSERT INTO filesystem.folders VALUES
    (10, 'Public', 1),
    (20, 'Confidential', 5),
    (30, 'TopSecret', 10);

-- Valid: File(Lvl 1) -> Public(Lvl 1) [1 <= 1 ✓]
-- Valid: File(Lvl 5) -> Confidential(Lvl 5) [5 <= 5 ✓]
-- Invalid: File(Lvl 10) -> Confidential(Lvl 5) [10 > 5 ✗]
-- Valid: File(Lvl 1) -> TopSecret(Lvl 10) [1 <= 10 ✓]
INSERT INTO filesystem.files VALUES
    (1, 'readme.txt', 1, 10),
    (2, 'budget.xls', 5, 20),
    (3, 'secret_codes.txt', 10, 20),
    (4, 'public_notes.txt', 1, 30);


-- ==========================================
-- 4. Denormalized Pattern (Airport -[FLIGHT]-> Airport)
-- Constraint: from.timezone_offset = to.timezone_offset (Same timezone flights)
-- Both nodes embedded in edge table
-- ==========================================

CREATE DATABASE IF NOT EXISTS travel;

DROP TABLE IF EXISTS travel.flights_denorm;

CREATE TABLE travel.flights_denorm (
    flight_id UInt32,
    flight_num String,
    
    -- Origin airport (from_node denormalized)
    origin_code String,
    origin_name String,
    origin_timezone_offset Int8,
    origin_country String,
    
    -- Destination airport (to_node denormalized)
    dest_code String,
    dest_name String,
    dest_timezone_offset Int8,
    dest_country String,
    
    departure_time DateTime,
    arrival_time DateTime
) ENGINE = MergeTree() ORDER BY flight_id;

-- Valid: JFK(UTC-5) -> BOS(UTC-5) [Same timezone ✓]
-- Valid: LAX(UTC-8) -> SFO(UTC-8) [Same timezone ✓]
-- Invalid: JFK(UTC-5) -> LAX(UTC-8) [Different timezones ✗]
-- Invalid: BOS(UTC-5) -> SFO(UTC-8) [Different timezones ✗]
INSERT INTO travel.flights_denorm VALUES
    (1, 'AA100', 'JFK', 'JFK Intl', -5, 'USA', 'BOS', 'Logan Intl', -5, 'USA', '2025-01-01 08:00:00', '2025-01-01 09:30:00'),
    (2, 'UA200', 'LAX', 'LAX Intl', -8, 'USA', 'SFO', 'SFO Intl', -8, 'USA', '2025-01-01 10:00:00', '2025-01-01 11:30:00'),
    (3, 'DL300', 'JFK', 'JFK Intl', -5, 'USA', 'LAX', 'LAX Intl', -8, 'USA', '2025-01-01 09:00:00', '2025-01-01 12:00:00'),
    (4, 'SW400', 'BOS', 'Logan Intl', -5, 'USA', 'SFO', 'SFO Intl', -8, 'USA', '2025-01-01 11:00:00', '2025-01-01 14:30:00');


-- ==========================================
-- 5. Polymorphic Edge Pattern (User -[INTERACTS]-> User)
-- Multiple edge types in one table with type discriminator
-- Constraint: from.reputation >= to.reputation (High-rep users interact with low-rep)
-- ==========================================

CREATE DATABASE IF NOT EXISTS community;

DROP TABLE IF EXISTS community.members;
DROP TABLE IF EXISTS community.interactions;

CREATE TABLE community.members (
    member_id UInt32,
    username String,
    reputation UInt32,
    join_date Date
) ENGINE = MergeTree() ORDER BY member_id;

CREATE TABLE community.interactions (
    from_member_id UInt32,
    to_member_id UInt32,
    interaction_type String,  -- 'MENTORS', 'REVIEWS', 'HELPS'
    interaction_date Date
) ENGINE = MergeTree() ORDER BY (from_member_id, to_member_id, interaction_type);

-- Members with varying reputation
INSERT INTO community.members VALUES
    (1, 'expert_alice', 1000, '2020-01-01'),
    (2, 'senior_bob', 500, '2021-01-01'),
    (3, 'junior_charlie', 100, '2023-01-01'),
    (4, 'newbie_dave', 10, '2024-01-01');

-- Valid: expert(1000) MENTORS junior(100) [1000 >= 100 ✓]
-- Valid: senior(500) HELPS newbie(10) [500 >= 10 ✓]
-- Invalid: junior(100) REVIEWS expert(1000) [100 < 1000 ✗]
-- Invalid: newbie(10) MENTORS senior(500) [10 < 500 ✗]
INSERT INTO community.interactions VALUES
    (1, 3, 'MENTORS', '2025-01-01'),
    (2, 4, 'HELPS', '2025-01-02'),
    (3, 1, 'REVIEWS', '2025-01-03'),
    (4, 2, 'MENTORS', '2025-01-04');
