-- Setup test data for lineage schema edge constraints testing
-- Creates database, tables, and sample data for data lineage tracking

-- Create database
CREATE DATABASE IF NOT EXISTS lineage;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS lineage.file_lineage;
DROP TABLE IF EXISTS lineage.file_analysis;
DROP TABLE IF EXISTS lineage.data_files;
DROP TABLE IF EXISTS lineage.analysis_results;

-- Create DataFile node table
CREATE TABLE lineage.data_files (
    file_id UInt32,
    file_path String,
    file_size_bytes UInt64,
    created_timestamp DateTime,
    pipeline_stage String,
    file_checksum String
) ENGINE = MergeTree()
ORDER BY file_id;

-- Create DataAnalysis node table
CREATE TABLE lineage.analysis_results (
    analysis_id UInt32,
    analysis_name String,
    run_timestamp DateTime,
    execution_status String
) ENGINE = MergeTree()
ORDER BY analysis_id;

-- Create COPIED_BY edge table
CREATE TABLE lineage.file_lineage (
    source_file_id UInt32,
    target_file_id UInt32,
    copy_operation_type String,
    operation_timestamp DateTime,
    operated_by_user String
) ENGINE = MergeTree()
ORDER BY (source_file_id, target_file_id);

-- Create ANALYZED_BY edge table
CREATE TABLE lineage.file_analysis (
    file_id UInt32,
    analysis_id UInt32,
    analysis_timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (file_id, analysis_id);

-- Insert test data: Linear pipeline raw -> bronze -> silver -> gold
-- Timeline: T0 (raw) -> T1 (bronze) -> T2 (silver) -> T3 (gold)
INSERT INTO lineage.data_files VALUES
    -- Raw stage (T0 = 2025-01-01 00:00:00)
    (1, '/data/raw/input.csv', 1024000, '2025-01-01 00:00:00', 'raw', 'abc123'),
    
    -- Bronze stage (T1 = 2025-01-01 01:00:00)
    (2, '/data/bronze/cleaned.csv', 950000, '2025-01-01 01:00:00', 'bronze', 'def456'),
    
    -- Silver stage (T2 = 2025-01-01 02:00:00)
    (3, '/data/silver/transformed.parquet', 850000, '2025-01-01 02:00:00', 'silver', 'ghi789'),
    
    -- Gold stage (T3 = 2025-01-01 03:00:00)
    (4, '/data/gold/aggregated.parquet', 120000, '2025-01-01 03:00:00', 'gold', 'jkl012'),
    
    -- Another raw file (T4 = 2025-01-01 04:00:00)
    (5, '/data/raw/input2.csv', 2048000, '2025-01-01 04:00:00', 'raw', 'mno345');

-- Insert lineage edges: 1 -> 2 -> 3 -> 4 (valid temporal order)
INSERT INTO lineage.file_lineage VALUES
    -- Raw -> Bronze (T0.5)
    (1, 2, 'copy', '2025-01-01 00:30:00', 'etl_bot'),
    
    -- Bronze -> Silver (T1.5)
    (2, 3, 'transform', '2025-01-01 01:30:00', 'etl_bot'),
    
    -- Silver -> Gold (T2.5)
    (3, 4, 'aggregate', '2025-01-01 02:30:00', 'etl_bot');

-- Insert analysis records
INSERT INTO lineage.analysis_results VALUES
    (1, 'Quality Check', '2025-01-01 03:30:00', 'completed'),
    (2, 'Anomaly Detection', '2025-01-01 04:00:00', 'running');

-- Insert file-to-analysis edges
INSERT INTO lineage.file_analysis VALUES
    (4, 1, '2025-01-01 03:30:00'),  -- Gold file analyzed
    (4, 2, '2025-01-01 04:00:00');  -- Gold file also analyzed by second analysis

-- Verify data
SELECT '=== DATA FILES ===' as info;
SELECT * FROM lineage.data_files ORDER BY file_id;

SELECT '=== FILE LINEAGE (COPIED_BY edges) ===' as info;
SELECT * FROM lineage.file_lineage ORDER BY source_file_id, target_file_id;

SELECT '=== ANALYSIS RESULTS ===' as info;
SELECT * FROM lineage.analysis_results ORDER BY analysis_id;

SELECT '=== FILE ANALYSIS (ANALYZED_BY edges) ===' as info;
SELECT * FROM lineage.file_analysis ORDER BY file_id, analysis_id;
