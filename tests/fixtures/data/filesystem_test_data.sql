-- Filesystem Test Data Fixture
-- Creates tables and loads test data for filesystem schema tests
-- Database: test_integration
-- Schema: schemas/examples/filesystem.yaml

-- Create database
CREATE DATABASE IF NOT EXISTS test_integration;

-- Drop and recreate tables
DROP TABLE IF EXISTS test_integration.fs_objects;
DROP TABLE IF EXISTS test_integration.fs_parent;

-- Create filesystem objects table
CREATE TABLE test_integration.fs_objects (
    object_id UInt32,
    name String,
    object_type String,
    size_bytes UInt64,
    mime_type Nullable(String),
    created_at DateTime,
    modified_at DateTime,
    owner_id UInt32
) ENGINE = MergeTree() ORDER BY object_id;

-- Create parent relationship table
CREATE TABLE test_integration.fs_parent (
    child_id UInt32,
    parent_id UInt32
) ENGINE = MergeTree() ORDER BY (child_id, parent_id);

-- Insert filesystem objects
INSERT INTO test_integration.fs_objects VALUES
(1, 'root', 'folder', 0, NULL, '2024-01-01 00:00:00', '2024-01-01 00:00:00', 1),
(2, 'Documents', 'folder', 0, NULL, '2024-01-02 00:00:00', '2024-01-02 00:00:00', 1),
(3, 'Pictures', 'folder', 0, NULL, '2024-01-02 00:00:00', '2024-01-02 00:00:00', 1),
(4, 'report.pdf', 'file', 102400, 'application/pdf', '2024-01-03 00:00:00', '2024-01-03 00:00:00', 1),
(5, 'notes.txt', 'file', 2048, 'text/plain', '2024-01-03 00:00:00', '2024-01-03 00:00:00', 1),
(6, 'vacation.jpg', 'file', 524288, 'image/jpeg', '2024-01-04 00:00:00', '2024-01-04 00:00:00', 1),
(7, 'Projects', 'folder', 0, NULL, '2024-01-05 00:00:00', '2024-01-05 00:00:00', 2),
(8, 'code.py', 'file', 4096, 'text/x-python', '2024-01-06 00:00:00', '2024-01-06 00:00:00', 2),
(9, 'data.csv', 'file', 819200, 'text/csv', '2024-01-07 00:00:00', '2024-01-07 00:00:00', 2),
(10, 'Archives', 'folder', 0, NULL, '2024-01-08 00:00:00', '2024-01-08 00:00:00', 1);

-- Insert parent relationships
INSERT INTO test_integration.fs_parent VALUES
(2, 1),
(3, 1),
(10, 1),
(4, 2),
(5, 2),
(6, 3),
(7, 2),
(8, 7),
(9, 7);

-- Verification queries (commented out, run manually if needed)
-- SELECT 'Filesystem objects count:', count(*) FROM test_integration.fs_objects;
-- SELECT 'Parent relationships count:', count(*) FROM test_integration.fs_parent;
-- SELECT 'Objects with parents:', count(DISTINCT child_id) FROM test_integration.fs_parent;
