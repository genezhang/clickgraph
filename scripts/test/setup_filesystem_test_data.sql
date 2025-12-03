-- File Storage System Test Data
-- Two tables: fs_objects (nodes) and fs_parent (edges)

CREATE TABLE IF NOT EXISTS test_integration.fs_objects (
    object_id UInt64,
    name String,
    object_type Enum8('folder' = 1, 'file' = 2),
    size_bytes UInt64 DEFAULT 0,
    mime_type Nullable(String),
    created_at DateTime DEFAULT now(),
    modified_at DateTime DEFAULT now(),
    owner_id UInt32
) ENGINE = MergeTree()
ORDER BY object_id;

CREATE TABLE IF NOT EXISTS test_integration.fs_parent (
    child_id UInt64,
    parent_id UInt64
) ENGINE = MergeTree()
ORDER BY (child_id, parent_id);

TRUNCATE TABLE test_integration.fs_objects;
TRUNCATE TABLE test_integration.fs_parent;

-- Folder hierarchy:
--   root(1)
--     ├── Documents(2)
--     │     ├── Work(5)
--     │     │     ├── report.pdf(11)
--     │     │     └── notes.txt(12)
--     │     └── Personal(6)
--     │           └── todo.txt(19)
--     ├── Projects(3)
--     │     ├── ProjectA(7)
--     │     │     ├── README.md(17)
--     │     │     ├── src(9)
--     │     │     │     ├── main.rs(15)
--     │     │     │     └── lib.rs(16)
--     │     │     └── docs(10)
--     │     │           └── design.pdf(18)
--     │     └── ProjectB(8)
--     │           └── budget.xlsx(20)
--     └── Pictures(4)
--           ├── vacation.jpg(13)
--           └── family.png(14)

-- Root folder (no parent - will not have entry in fs_parent)
INSERT INTO test_integration.fs_objects (object_id, name, object_type, owner_id) VALUES
    (1, 'root', 'folder', 1);

-- Top-level folders under root
INSERT INTO test_integration.fs_objects (object_id, name, object_type, owner_id) VALUES
    (2, 'Documents', 'folder', 1),
    (3, 'Projects', 'folder', 1),
    (4, 'Pictures', 'folder', 1);

-- Subfolders
INSERT INTO test_integration.fs_objects (object_id, name, object_type, owner_id) VALUES
    (5, 'Work', 'folder', 1),
    (6, 'Personal', 'folder', 1),
    (7, 'ProjectA', 'folder', 1),
    (8, 'ProjectB', 'folder', 1),
    (9, 'src', 'folder', 1),
    (10, 'docs', 'folder', 1);

-- Files
INSERT INTO test_integration.fs_objects (object_id, name, object_type, size_bytes, mime_type, owner_id) VALUES
    (11, 'report.pdf', 'file', 1048576, 'application/pdf', 1),
    (12, 'notes.txt', 'file', 2048, 'text/plain', 1),
    (13, 'vacation.jpg', 'file', 5242880, 'image/jpeg', 1),
    (14, 'family.png', 'file', 3145728, 'image/png', 1),
    (15, 'main.rs', 'file', 4096, 'text/x-rust', 1),
    (16, 'lib.rs', 'file', 8192, 'text/x-rust', 1),
    (17, 'README.md', 'file', 1024, 'text/markdown', 1),
    (18, 'design.pdf', 'file', 2097152, 'application/pdf', 1),
    (19, 'todo.txt', 'file', 512, 'text/plain', 1),
    (20, 'budget.xlsx', 'file', 65536, 'application/vnd.ms-excel', 1);

-- Parent relationships (child_id -> parent_id)
INSERT INTO test_integration.fs_parent (child_id, parent_id) VALUES
    (2, 1),
    (3, 1),
    (4, 1),
    (5, 2),
    (6, 2),
    (7, 3),
    (8, 3),
    (9, 7),
    (10, 7),
    (11, 5),
    (12, 5),
    (13, 4),
    (14, 4),
    (15, 9),
    (16, 9),
    (17, 7),
    (18, 10),
    (19, 6),
    (20, 8);

SELECT 'Total objects' as info, count(*) as cnt FROM test_integration.fs_objects;
SELECT 'Folders' as info, count(*) as cnt FROM test_integration.fs_objects WHERE object_type = 'folder';
SELECT 'Files' as info, count(*) as cnt FROM test_integration.fs_objects WHERE object_type = 'file';
