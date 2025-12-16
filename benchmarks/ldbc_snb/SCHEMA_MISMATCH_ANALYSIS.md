# LDBC Schema Mismatch Analysis

**Date**: December 15, 2025  
**Issue**: Original DDL didn't match actual LDBC CSV data structure

## Root Cause

The LDBC SNB benchmark documentation and the actual generated CSV files use **different naming conventions**:

- **Documentation/Spec**: Uses underscore notation (`Person_id`, `Tag_id`)
- **Actual CSV Files**: Uses CamelCase notation (`PersonId`, `TagId`)

This caused all edge tables to load with zero values when using `TabSeparatedWithNames` format, since ClickHouse couldn't match column names.

## Key Differences

### 1. **Column Naming Convention**

| DDL (Original - WRONG) | CSV (Actual - CORRECT) |
|------------------------|------------------------|
| `Person_id` | `PersonId` |
| `Tag_id` | `TagId` |
| `Post_id` | `PostId` |
| `Comment_id` | `CommentId` |
| `Forum_id` | `ForumId` |
| `Place_id` | `Place1Id`, `Place2Id`, `CityId`, `CountryId` |
| `Organisation_id` | `OrganisationId`, `CompanyId`, `UniversityId` |

### 2. **Missing creationDate Columns**

Many edge tables have `creationDate` as the FIRST column in CSV but it was missing in original DDL:

**Tables affected:**
- `Person_isLocatedIn_Place`
- `Person_hasInterest_Tag`
- `Person_workAt_Organisation`
- `Person_studyAt_Organisation`
- `Forum_hasModerator_Person`
- `Forum_hasMember_Person`
- `Forum_hasTag_Tag`
- `Post_hasCreator_Person`
- `Post_isLocatedIn_Place`
- `Post_hasTag_Tag`
- `Forum_containerOf_Post`
- `Comment_hasCreator_Person`
- `Comment_isLocatedIn_Place`
- `Comment_hasTag_Tag`
- `Comment_replyOf_Post`
- `Comment_replyOf_Comment`

### 3. **Column Order Changes**

Some tables had wrong column order:

**Forum**: CSV has `creationDate|id|title`, DDL had `id|title|creationDate`  
**Organisation**: CSV has `id|type|name|url`, DDL had `id|name|url|type`  
**Post**: CSV has `creationDate` first, DDL had `id` first  
**Comment**: CSV has `creationDate` first, DDL had `id` first

### 4. **Special Place Naming**

CSV files use specific place type names:
- `Person_isLocatedIn_City` directory → `CityId` column (not `PlaceId`)
- `Post_isLocatedIn_Country` directory → `CountryId` column (not `PlaceId`)
- `Comment_isLocatedIn_Country` directory → `CountryId` column (not `PlaceId`)

But all map to the generic `Place` table with a `type` column.

## Impact

**Before fix:**
- All edge tables loaded with 0 values
- Person_isLocatedIn_Place: 67,110 rows of zeros + 67,110 correct rows (mixed)
- All other edge tables: 100% zeros
- All LDBC queries failed (0/33 passing)

**After fix:**
- Edge tables load correctly with actual data
- LDBC queries should work with proper relationships

## Solution

Created `clickhouse_ddl_corrected.sql` that matches actual CSV structure exactly:
- Uses CamelCase column names (`PersonId`, `TagId`, etc.)
- Includes `creationDate` in all edge tables
- Correct column order matching CSV
- Can now use `TabSeparatedWithNames` format for simple loading

## Lesson Learned

**Always validate against actual data files, not just documentation!**

The LDBC specification may differ from the actual CSV output. Always:
1. Check CSV headers with `head -1 file.csv`
2. Verify data types and column order
3. Test loading a small sample before full dataset
4. Validate row counts and sample data after loading
