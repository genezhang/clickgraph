# Changelog

## [Unreleased] - 2025-10-17

### 🚀 Features

- **OPTIONAL MATCH Support**: Full implementation of LEFT JOIN semantics for optional graph patterns
  - Two-word keyword parsing (`OPTIONAL MATCH`)
  - Optional alias tracking in query planner
  - Automatic LEFT JOIN generation in SQL
  - 11/11 tests passing (100% coverage)
  - Complete documentation in `docs/optional-match-guide.md`

- **YAML Schema Improvements**: Fixed label and type_name field handling
  - Server now uses `node_mapping.label` instead of HashMap keys
  - Relationship `from_node_type`/`to_node_type` properly loaded from YAML
  - Schema loads correctly with User nodes and FRIENDS_WITH relationships

### 📚 Documentation

- Added `docs/optional-match-guide.md` - Comprehensive OPTIONAL MATCH feature guide
- Added `OPTIONAL_MATCH_COMPLETE.md` - Technical implementation details
- Added `YAML_SCHEMA_INVESTIGATION.md` - YAML schema fixes and discoveries
- Updated `STATUS_REPORT.md` - Added OPTIONAL MATCH to feature matrix
- Updated `README.md` - Added OPTIONAL MATCH examples
- Updated `.github/copilot-instructions.md` - Windows constraints and OPTIONAL MATCH status

### 🐛 Bug Fixes

- Fixed YAML schema loading to use proper label/type_name fields
- Fixed relationship from/to node type mapping in graph_catalog.rs

### 🧪 Testing

- Test data creation with Windows Memory engine constraint
- 261/262 tests passing (99.6% overall)
- 11/11 OPTIONAL MATCH-specific tests (100%)

### ⚙️ Infrastructure

- Documented Windows environment constraints (Docker volume permissions, curl alternatives)
- Created `setup_test_data.sql` for test data with Memory engine

## [0.0.4] - 2025-09-18

### 🚀 Features

- Query planner rewrite (#11)

### 🐛 Bug Fixes

- Count start issue (#6)

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
## [0.0.3] - 2025-06-29

### 🚀 Features

- :sparkles: support for multi node conditions
- Support for multi node conditions

### 🐛 Bug Fixes

- :bug: relation direction when same node types
- :bug: Property tagging to node name
- :bug: node name in return clause related issues

### 💼 Other

- Node name in return clause related issues

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
- Update CHANGELOG.md [skip ci]
## [0.0.2] - 2025-06-27

### 🚀 Features

- :sparkles: Added basic schema inferenc

### ⚙️ Miscellaneous Tasks

- Update CHANGELOG.md [skip ci]
## [0.0.1] - 2025-05-28

### ⚙️ Miscellaneous Tasks

- Fixed docker pipeline mac issue
- Fixed docker mac issue
- Fixed docker image mac issue
