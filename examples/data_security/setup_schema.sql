-- Data Security Schema - Denormalized Tables
-- Uses parent_id in fs_objects instead of separate edge table
-- Run with: curl -s "http://localhost:8123/?user=test_user&password=test_pass" --data-binary @scripts/setup/data_security_schema.sql

-- Drop existing tables if they exist
DROP TABLE IF EXISTS data_security.ds_users;
DROP TABLE IF EXISTS data_security.ds_groups;
DROP TABLE IF EXISTS data_security.ds_memberships;
DROP TABLE IF EXISTS data_security.ds_fs_objects;
DROP TABLE IF EXISTS data_security.ds_permissions;

-- Users table
CREATE TABLE data_security.ds_users (
    user_id UInt64,
    name String,
    email String,
    department String,
    exposure String  -- 'internal' or 'external'
) ENGINE = MergeTree()
ORDER BY user_id;

-- Groups table
CREATE TABLE data_security.ds_groups (
    group_id UInt64,
    name String,
    description String
) ENGINE = MergeTree()
ORDER BY group_id;

-- Memberships (User/Group -> Group)
CREATE TABLE data_security.ds_memberships (
    member_id UInt64,
    group_id UInt64,
    member_type String  -- 'User' or 'Group'
) ENGINE = MergeTree()
ORDER BY (member_id, group_id);

-- File System Objects (denormalized with parent_id)
-- parent_id = 0 means root folder (ClickHouse doesn't have NULL for UInt64, use 0)
CREATE TABLE data_security.ds_fs_objects (
    fs_id UInt64,
    fs_type String,           -- 'Folder' or 'File'
    name String,
    path String,
    parent_id UInt64 DEFAULT 0,  -- 0 = root folder
    sensitive_data UInt8 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY fs_id;

-- Permissions (User/Group -> Folder/File)
CREATE TABLE data_security.ds_permissions (
    subject_id UInt64,
    object_id UInt64,
    subject_type String,  -- 'User' or 'Group'
    object_type String,   -- 'Folder' or 'File'
    privilege String      -- 'read', 'write', 'execute', 'admin'
) ENGINE = MergeTree()
ORDER BY (subject_id, object_id);

-- ============================================================================
-- TEST DATA
-- ============================================================================

-- Insert Users (200 users, ~20% external)
INSERT INTO data_security.ds_users
SELECT 
    number AS user_id,
    concat('User_', toString(number)) AS name,
    concat('user', toString(number), '@example.com') AS email,
    arrayElement(['Engineering', 'Sales', 'HR', 'Finance', 'IT'], (number % 5) + 1) AS department,
    if(number % 5 = 0, 'external', 'internal') AS exposure
FROM numbers(1, 200);

-- Insert Groups (50 groups)
INSERT INTO data_security.ds_groups
SELECT 
    number AS group_id,
    concat('Group_', toString(number)) AS name,
    concat('Description for group ', toString(number)) AS description
FROM numbers(1, 50);

-- Insert Memberships (users -> groups)
INSERT INTO data_security.ds_memberships
SELECT 
    number AS member_id,
    ((number - 1) % 50) + 1 AS group_id,  -- Each user in one group
    'User' AS member_type
FROM numbers(1, 200);

-- Additional memberships (some users in multiple groups)
INSERT INTO data_security.ds_memberships
SELECT 
    number AS member_id,
    ((number - 1) % 25) + 26 AS group_id,  -- Second group for half the users
    'User' AS member_type
FROM numbers(1, 100);

-- Insert Root Folders (10 root folders, parent_id = 0, is_root = 1)
-- Insert Root Folders (10 root folders, parent_id = 0)
INSERT INTO data_security.ds_fs_objects (fs_id, fs_type, name, path, parent_id, sensitive_data)
SELECT 
    number AS fs_id,
    'Folder' AS fs_type,
    concat('Root_', toString(number)) AS name,
    concat('/', 'Root_', toString(number)) AS path,
    0 AS parent_id,
    0 AS sensitive_data
FROM numbers(1, 10);

-- Insert Level 1 Folders (30 folders under roots)
INSERT INTO data_security.ds_fs_objects (fs_id, fs_type, name, path, parent_id, sensitive_data)
SELECT 
    100 + number AS fs_id,
    'Folder' AS fs_type,
    concat('Folder_L1_', toString(number)) AS name,
    concat('/Root_', toString(((number - 1) % 10) + 1), '/Folder_L1_', toString(number)) AS path,
    ((number - 1) % 10) + 1 AS parent_id,  -- Points to root folders 1-10
    0 AS sensitive_data
FROM numbers(1, 30);

-- Insert Level 2 Folders (60 folders under L1)
INSERT INTO data_security.ds_fs_objects (fs_id, fs_type, name, path, parent_id, sensitive_data)
SELECT 
    200 + number AS fs_id,
    'Folder' AS fs_type,
    concat('Folder_L2_', toString(number)) AS name,
    concat('/path/to/Folder_L2_', toString(number)) AS path,
    100 + ((number - 1) % 30) + 1 AS parent_id,  -- Points to L1 folders
    0 AS sensitive_data
FROM numbers(1, 60);

-- Insert Files (200 files distributed across folders, ~30% sensitive)
INSERT INTO data_security.ds_fs_objects (fs_id, fs_type, name, path, parent_id, sensitive_data)
SELECT 
    1000 + number AS fs_id,
    'File' AS fs_type,
    concat('File_', toString(number), '.txt') AS name,
    concat('/path/to/File_', toString(number), '.txt') AS path,
    -- Distribute across all folders (roots, L1, L2)
    multiIf(
        number <= 50, ((number - 1) % 10) + 1,           -- In root folders
        number <= 120, 100 + ((number - 51) % 30) + 1,   -- In L1 folders
        200 + ((number - 121) % 60) + 1                   -- In L2 folders
    ) AS parent_id,
    if(number % 3 = 0, 1, 0) AS sensitive_data  -- ~33% sensitive
FROM numbers(1, 200);

-- Insert Permissions (User -> Folder, unique permissions)
INSERT INTO data_security.ds_permissions
SELECT DISTINCT
    number AS subject_id,
    ((number - 1) % 10) + 1 AS object_id,  -- Access to root folders
    'User' AS subject_type,
    'Folder' AS object_type,
    arrayElement(['read', 'write', 'execute'], (number % 3) + 1) AS privilege
FROM numbers(1, 100);

-- Insert Permissions (Group -> Folder)
INSERT INTO data_security.ds_permissions
SELECT DISTINCT
    number AS subject_id,
    100 + ((number - 1) % 30) + 1 AS object_id,  -- Access to L1 folders
    'Group' AS subject_type,
    'Folder' AS object_type,
    'read' AS privilege
FROM numbers(1, 50);

-- Insert Permissions (User -> File, direct file access)
INSERT INTO data_security.ds_permissions
SELECT DISTINCT
    number AS subject_id,
    1000 + ((number - 1) % 50) + 1 AS object_id,  -- Direct access to some files
    'User' AS subject_type,
    'File' AS object_type,
    'read' AS privilege
FROM numbers(1, 50);

SELECT 'Data Security schema created successfully!' AS status;
SELECT 'Users:', count() FROM data_security.ds_users;
SELECT 'Groups:', count() FROM data_security.ds_groups;
SELECT 'Memberships:', count() FROM data_security.ds_memberships;
SELECT 'FS Objects:', count() FROM data_security.ds_fs_objects;
SELECT 'Root Folders:', countIf(parent_id = 0 AND fs_type = 'Folder') FROM data_security.ds_fs_objects;
SELECT 'Files:', countIf(fs_type = 'File') FROM data_security.ds_fs_objects;
SELECT 'Sensitive Files:', countIf(fs_type = 'File' AND sensitive_data = 1) FROM data_security.ds_fs_objects;
SELECT 'Permissions:', count() FROM data_security.ds_permissions;
