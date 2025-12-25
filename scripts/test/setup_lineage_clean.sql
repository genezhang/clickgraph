CREATE DATABASE IF NOT EXISTS lineage;

DROP TABLE IF EXISTS lineage.file_lineage;
DROP TABLE IF EXISTS lineage.file_analysis;
DROP TABLE IF EXISTS lineage.data_files;
DROP TABLE IF EXISTS lineage.analysis_results;

CREATE TABLE lineage.data_files (
    file_id UInt32,
    file_path String,
    file_size_bytes UInt64,
    created_timestamp DateTime,
    pipeline_stage String,
    file_checksum String
) ENGINE = MergeTree()
ORDER BY file_id;

CREATE TABLE lineage.analysis_results (
    analysis_id UInt32,
    analysis_name String,
    run_timestamp DateTime,
    execution_status String
) ENGINE = MergeTree()
ORDER BY analysis_id;

CREATE TABLE lineage.file_lineage (
    source_file_id UInt32,
    target_file_id UInt32,
    copy_operation_type String,
    operation_timestamp DateTime,
    operated_by_user String
) ENGINE = MergeTree()
ORDER BY (source_file_id, target_file_id);

CREATE TABLE lineage.file_analysis (
    file_id UInt32,
    analysis_id UInt32,
    analysis_timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (file_id, analysis_id);

INSERT INTO lineage.data_files VALUES
    (1, '/data/raw/input.csv', 1024000, '2025-01-01 00:00:00', 'raw', 'abc123'),
    (2, '/data/bronze/cleaned.csv', 950000, '2025-01-01 01:00:00', 'bronze', 'def456'),
    (3, '/data/silver/transformed.parquet', 850000, '2025-01-01 02:00:00', 'silver', 'ghi789'),
    (4, '/data/gold/aggregated.parquet', 120000, '2025-01-01 03:00:00', 'gold', 'jkl012'),
    (5, '/data/raw/input2.csv', 2048000, '2025-01-01 04:00:00', 'raw', 'mno345');

INSERT INTO lineage.file_lineage VALUES
    (1, 2, 'copy', '2025-01-01 00:30:00', 'etl_bot'),
    (2, 3, 'transform', '2025-01-01 01:30:00', 'etl_bot'),
    (3, 4, 'aggregate', '2025-01-01 02:30:00', 'etl_bot');

INSERT INTO lineage.analysis_results VALUES
    (1, 'Quality Check', '2025-01-01 03:30:00', 'completed'),
    (2, 'Anomaly Detection', '2025-01-01 04:00:00', 'running');

INSERT INTO lineage.file_analysis VALUES
    (4, 1, '2025-01-01 03:30:00'),
    (4, 2, '2025-01-01 04:00:00');
