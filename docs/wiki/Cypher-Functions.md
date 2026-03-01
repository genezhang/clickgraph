# Cypher Aggregations & Functions

Complete reference for aggregation functions, string operations, date/time functions, and mathematical operations in ClickGraph.

## Table of Contents
- [Aggregation Functions](#aggregation-functions)
- [String Functions](#string-functions)
- [Date and Time Functions](#date-and-time-functions)
- [Mathematical Functions](#mathematical-functions)
- [Type Conversion Functions](#type-conversion-functions)
- [List Functions](#list-functions)
- [Scalar Functions](#scalar-functions)
- [Complete Function Reference](#complete-function-reference)
- [ClickHouse Function Pass-Through](#clickhouse-function-pass-through) ⭐ **NEW**

---

## Aggregation Functions

Aggregation functions compute summary statistics over groups of rows.

### COUNT

Count number of rows or non-null values.

```cypher
-- Count all matching nodes
MATCH (u:User)
RETURN count(u) AS total_users

-- Count with filtering
MATCH (u:User)
WHERE u.age > 30
RETURN count(u) AS users_over_30

-- Count distinct values
MATCH (u:User)
RETURN count(DISTINCT u.country) AS num_countries

-- Count edges
MATCH (:User)-[e:FOLLOWS]->(:User)
RETURN count(e) AS total_follows

-- Count with grouping
MATCH (u:User)
RETURN u.country, count(u) AS users_per_country
ORDER BY users_per_country DESC
```

**Special forms**:
- `count(*)` - Count all rows (including nulls)
- `count(expr)` - Count non-null values
- `count(DISTINCT expr)` - Count unique non-null values

### SUM

Sum numeric values.

```cypher
-- Sum follower counts
MATCH (u:User)<-[:FOLLOWS]-()
RETURN u.name, count(*) AS followers

-- Sum ages
MATCH (u:User)
RETURN sum(u.age) AS total_age

-- Sum by group
MATCH (u:User)
RETURN u.country, sum(u.age) AS total_age_by_country

-- Sum with calculation
MATCH (u:User)
WHERE u.country = 'USA'
RETURN sum(u.age) / count(u) AS avg_age
```

### AVG

Calculate average of numeric values.

```cypher
-- Average age
MATCH (u:User)
RETURN avg(u.age) AS average_age

-- Average by country
MATCH (u:User)
RETURN u.country, avg(u.age) AS avg_age
ORDER BY avg_age DESC

-- Average follower count
MATCH (u:User)
WITH u, count{(u)<-[:FOLLOWS]-()} AS followers
RETURN avg(followers) AS avg_followers
```

### MIN and MAX

Find minimum and maximum values.

```cypher
-- Min and max age
MATCH (u:User)
RETURN min(u.age) AS youngest, max(u.age) AS oldest

-- Oldest user per country
MATCH (u:User)
RETURN u.country, max(u.age) AS oldest_age
ORDER BY oldest_age DESC

-- Date ranges
MATCH (u:User)
RETURN min(u.registration_date) AS first_registration,
       max(u.registration_date) AS latest_registration
```

### COLLECT

Aggregate values into a list.

```cypher
-- Collect all names
MATCH (u:User)
WHERE u.country = 'USA'
RETURN collect(u.name) AS usa_users

-- Collect with distinct
MATCH (u:User)
RETURN collect(DISTINCT u.country) AS countries

-- Collect friend names
MATCH (me:User {name: 'Alice'})-[:FOLLOWS]->(friend)
RETURN collect(friend.name) AS friends

-- Limited collection
MATCH (u:User)
RETURN u.country, collect(u.name)[0..5] AS sample_users
```

### GROUP BY (Implicit)

Cypher uses implicit GROUP BY - non-aggregated columns in RETURN are grouping keys.

```cypher
-- Group by country (implicit)
MATCH (u:User)
RETURN u.country, count(u) AS user_count
-- Equivalent SQL: SELECT country, COUNT(*) FROM users GROUP BY country

-- Multiple grouping columns
MATCH (u:User)
RETURN u.country, u.city, count(u) AS user_count
ORDER BY user_count DESC

-- Group with filtering
MATCH (u:User)
WHERE u.age > 25
RETURN u.country, avg(u.age) AS avg_age, count(u) AS count
ORDER BY count DESC
```

### HAVING (via WHERE on aggregates)

Filter groups after aggregation using WITH + WHERE:

```cypher
-- Countries with more than 10 users
MATCH (u:User)
WITH u.country AS country, count(u) AS user_count
WHERE user_count > 10
RETURN country, user_count
ORDER BY user_count DESC

-- Users with more than 5 followers
MATCH (u:User)<-[:FOLLOWS]-()
WITH u, count(*) AS followers
WHERE followers > 5
RETURN u.name, followers
ORDER BY followers DESC
```

---

## String Functions

### Case Conversion

```cypher
-- Convert to lowercase
MATCH (u:User)
RETURN toLower(u.name) AS lowercase_name

-- Convert to uppercase
MATCH (u:User)
RETURN toUpper(u.name) AS uppercase_name

-- Case-insensitive search
MATCH (u:User)
WHERE toLower(u.name) = 'alice'
RETURN u.name
```

**Functions**:
- `toLower(str)` - Convert to lowercase
- `toUpper(str)` - Convert to uppercase

### Trimming and Cleaning

```cypher
-- Remove leading/trailing whitespace
MATCH (u:User)
RETURN trim(u.name) AS clean_name

-- Left trim (leading whitespace)
MATCH (u:User)
RETURN ltrim(u.name) AS left_trimmed

-- Right trim (trailing whitespace)
MATCH (u:User)
RETURN rtrim(u.name) AS right_trimmed
```

### String Concatenation

```cypher
-- Concatenate strings
MATCH (u:User)
RETURN u.name + ' (' + u.country + ')' AS user_info

-- Build full name
MATCH (u:User)
RETURN u.first_name + ' ' + u.last_name AS full_name

-- Multiple concatenations
MATCH (u:User)
RETURN u.name + ', ' + u.city + ', ' + u.country AS location_string
```

### Substring Operations

```cypher
-- Extract substring (start index, length)
MATCH (u:User)
RETURN substring(u.name, 0, 3) AS first_three_chars

-- Get substring from position to end
MATCH (u:User)
RETURN substring(u.name, 1) AS all_but_first_char

-- Left/right substrings
MATCH (u:User)
RETURN left(u.email, 10) AS email_prefix,
       right(u.email, 10) AS email_suffix
```

**Note**: String indexes are 0-based in Cypher

### String Length

```cypher
-- String length
MATCH (u:User)
RETURN u.name, size(u.name) AS name_length
ORDER BY name_length DESC

-- Filter by length
MATCH (u:User)
WHERE size(u.name) > 10
RETURN u.name
```

### String Splitting

```cypher
-- Split string into list
MATCH (u:User)
RETURN split(u.full_name, ' ') AS name_parts

-- Get first and last name
MATCH (u:User)
RETURN split(u.full_name, ' ')[0] AS first_name,
       split(u.full_name, ' ')[1] AS last_name

-- Split email
MATCH (u:User)
RETURN split(u.email, '@')[0] AS username,
       split(u.email, '@')[1] AS domain
```

### String Replacement

```cypher
-- Replace substring
MATCH (u:User)
RETURN replace(u.email, '@example.com', '@newdomain.com') AS new_email

-- Remove substring (replace with empty)
MATCH (u:User)
RETURN replace(u.phone, '-', '') AS phone_digits_only
```

### String Reversal

```cypher
-- Reverse string
MATCH (u:User)
RETURN reverse(u.name) AS reversed_name
```

---

## Date and Time Functions

ClickGraph maps Neo4j date/time functions to ClickHouse equivalents.

### Current Date and Time

```cypher
-- Current datetime
RETURN datetime() AS now

-- Current date only
RETURN date() AS today

-- Current time only
RETURN time() AS current_time

-- Current timestamp
RETURN timestamp() AS unix_timestamp
```

### Date Construction

```cypher
-- Create date from components
RETURN date({year: 2025, month: 11, day: 17}) AS specific_date

-- Create datetime
RETURN datetime({year: 2025, month: 11, day: 17, hour: 14, minute: 30}) AS dt

-- Parse date string
RETURN date('2025-11-17') AS parsed_date

-- Parse datetime string
RETURN datetime('2025-11-17T14:30:00') AS parsed_datetime
```

### Date Extraction

```cypher
-- Extract year, month, day
MATCH (u:User)
RETURN u.name,
       u.registration_date.year AS reg_year,
       u.registration_date.month AS reg_month,
       u.registration_date.day AS reg_day

-- Extract time components
MATCH (u:User)
RETURN u.name,
       u.last_login.hour AS login_hour,
       u.last_login.minute AS login_minute

-- Day of week
MATCH (u:User)
RETURN u.name, u.registration_date.dayOfWeek AS weekday
```

### Date Arithmetic

```cypher
-- Add duration to date
MATCH (u:User)
RETURN u.name,
       u.registration_date + duration({days: 30}) AS expiry_date

-- Subtract duration
MATCH (u:User)
RETURN u.name,
       datetime() - u.registration_date AS account_age

-- Duration between dates
MATCH (u:User)
RETURN u.name,
       duration.between(u.registration_date, datetime()) AS account_duration
```

### Duration Construction

```cypher
-- Create duration
RETURN duration({days: 7}) AS one_week

-- Multiple units
RETURN duration({days: 1, hours: 12, minutes: 30}) AS custom_duration

-- Parse duration string
RETURN duration('P1Y2M3DT4H5M6S') AS iso_duration
-- P1Y2M3D = 1 year, 2 months, 3 days
-- T4H5M6S = 4 hours, 5 minutes, 6 seconds
```

### Date Formatting

```cypher
-- Format date as string
MATCH (u:User)
RETURN u.name, toString(u.registration_date) AS reg_date_str

-- Custom format (via ClickHouse)
MATCH (u:User)
RETURN u.name,
       formatDateTime(u.registration_date, '%Y-%m-%d') AS formatted_date
```

### Date Comparisons

```cypher
-- Filter by date range
MATCH (u:User)
WHERE u.registration_date > date('2024-01-01')
  AND u.registration_date < date('2025-01-01')
RETURN u.name, u.registration_date

-- Recent registrations
MATCH (u:User)
WHERE u.registration_date > datetime() - duration({days: 30})
RETURN u.name, u.registration_date
ORDER BY u.registration_date DESC
```

---

## Mathematical Functions

### Basic Math

```cypher
-- Absolute value
MATCH (u:User)
RETURN abs(u.account_balance) AS abs_balance

-- Sign (-1, 0, 1)
MATCH (u:User)
RETURN sign(u.account_balance) AS balance_sign

-- Power
MATCH (u:User)
RETURN u.age, pow(u.age, 2) AS age_squared

-- Square root
MATCH (u:User)
RETURN u.age, sqrt(u.age) AS age_sqrt
```

### Rounding Functions

```cypher
-- Round to nearest integer
MATCH (u:User)
RETURN round(u.rating) AS rounded_rating

-- Round up (ceiling)
MATCH (u:User)
RETURN ceil(u.rating) AS rating_ceiling

-- Round down (floor)
MATCH (u:User)
RETURN floor(u.rating) AS rating_floor

-- Round to decimal places
MATCH (u:User)
RETURN round(u.rating * 100) / 100 AS rating_2dp
```

### Trigonometric Functions

```cypher
-- Sine, cosine, tangent
RETURN sin(3.14159) AS sine,
       cos(3.14159) AS cosine,
       tan(3.14159) AS tangent

-- Inverse functions
RETURN asin(0.5) AS arcsine,
       acos(0.5) AS arccosine,
       atan(1.0) AS arctangent
```

### Logarithmic Functions

```cypher
-- Natural logarithm
MATCH (u:User)
RETURN log(u.follower_count) AS log_followers

-- Base-10 logarithm
MATCH (u:User)
RETURN log10(u.follower_count) AS log10_followers

-- Exponential
RETURN exp(2.0) AS e_squared
```

### Random Numbers

```cypher
-- Random float between 0 and 1
RETURN rand() AS random_value

-- Random integer in range [0, n)
RETURN floor(rand() * 100) AS random_0_to_99

-- Random sample of users
MATCH (u:User)
WHERE rand() < 0.1  -- 10% sample
RETURN u.name
LIMIT 10
```

---

## Type Conversion Functions

### To Integer

```cypher
-- String to integer
RETURN toInteger('42') AS num

-- Float to integer
MATCH (u:User)
RETURN toInteger(u.rating) AS rating_int

-- Boolean to integer (true=1, false=0)
MATCH (u:User)
RETURN toInteger(u.is_active) AS active_int
```

### To Float

```cypher
-- String to float
RETURN toFloat('3.14') AS num

-- Integer to float
MATCH (u:User)
RETURN toFloat(u.age) / 10.0 AS age_scaled
```

### To String

```cypher
-- Integer to string
MATCH (u:User)
RETURN toString(u.age) AS age_str

-- Date to string
MATCH (u:User)
RETURN toString(u.registration_date) AS reg_date_str

-- Boolean to string
MATCH (u:User)
RETURN toString(u.is_active) AS active_str
```

### To Boolean

```cypher
-- String to boolean
RETURN toBoolean('true') AS bool_val

-- Integer to boolean (0=false, non-zero=true)
MATCH (u:User)
RETURN toBoolean(u.login_count) AS has_logged_in
```

---

## List Functions

### List Operations

```cypher
-- List size/length
MATCH (u:User)
RETURN u.name, size(u.interests) AS num_interests

-- Check if element in list
MATCH (u:User)
WHERE 'sports' IN u.interests
RETURN u.name

-- List concatenation
RETURN [1, 2, 3] + [4, 5, 6] AS combined_list

-- List slicing
RETURN [1, 2, 3, 4, 5][1..3] AS slice  -- Returns [2, 3]

-- First/last element
RETURN head([1, 2, 3, 4, 5]) AS first,
       last([1, 2, 3, 4, 5]) AS last_elem

-- All but first/last
RETURN tail([1, 2, 3, 4, 5]) AS all_but_first
```

### List Comprehensions

```cypher
-- Transform list elements
MATCH (u:User)
RETURN [interest IN u.interests | toUpper(interest)] AS upper_interests

-- Filter list elements
MATCH (u:User)
RETURN [interest IN u.interests WHERE size(interest) > 5] AS long_interests

-- Combined transform and filter
MATCH path = (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE a.name = 'Alice'
RETURN [node IN nodes(path) WHERE node.country = 'USA' | node.name] AS usa_users
```

### List Predicates

```cypher
-- All elements satisfy condition
MATCH path = (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE all(node IN nodes(path) WHERE node.is_active = true)
RETURN b.name

-- Any element satisfies condition
MATCH path = (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE any(node IN nodes(path) WHERE node.country = 'USA')
RETURN b.name

-- No elements satisfy condition
MATCH path = (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE none(node IN nodes(path) WHERE node.is_blocked = true)
RETURN b.name

-- At least one element satisfies condition
MATCH path = (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE single(node IN nodes(path) WHERE node.country = 'USA')
RETURN b.name
```

### List Reduction

```cypher
-- Sum list elements
RETURN reduce(total = 0, x IN [1, 2, 3, 4, 5] | total + x) AS sum

-- Product
RETURN reduce(prod = 1, x IN [1, 2, 3, 4, 5] | prod * x) AS product

-- Custom aggregation
MATCH path = (a:User)-[:FOLLOWS*1..3]->(b:User)
WHERE a.name = 'Alice'
RETURN reduce(ages = 0, node IN nodes(path) | ages + node.age) AS total_age
```

---

## Scalar Functions

### COALESCE

Return first non-null value:

```cypher
-- Provide default for null
MATCH (u:User)
RETURN u.name, COALESCE(u.email, 'no-email@example.com') AS email

-- Multiple fallbacks
MATCH (u:User)
RETURN COALESCE(u.nickname, u.full_name, u.username, 'Anonymous') AS display_name
```

### CASE Expressions

Conditional logic:

```cypher
-- Simple CASE
MATCH (u:User)
RETURN u.name,
       CASE u.country
         WHEN 'USA' THEN 'North America'
         WHEN 'Canada' THEN 'North America'
         WHEN 'Mexico' THEN 'North America'
         ELSE 'Other'
       END AS region

-- Searched CASE (with conditions)
MATCH (u:User)
RETURN u.name,
       CASE
         WHEN u.age < 18 THEN 'Minor'
         WHEN u.age < 65 THEN 'Adult'
         ELSE 'Senior'
       END AS age_group

-- CASE in WHERE clause
MATCH (u:User)
WHERE CASE
        WHEN u.country = 'USA' THEN u.age >= 21
        ELSE u.age >= 18
      END
RETURN u.name
```

### Type Checking

```cypher
-- Check node label
MATCH (n)
RETURN CASE
  WHEN n:User THEN 'User'
  WHEN n:Post THEN 'Post'
  ELSE 'Other'
END AS node_type

-- Check edge type
MATCH ()-[e]->()
RETURN type(e) AS edge_type, count(*) AS count
GROUP BY type(e)
```

### ID Functions

```cypher
-- Get node ID
MATCH (u:User)
RETURN id(u), u.name

-- Note: id() returns internal ID, use properties for business keys
MATCH (u:User)
RETURN u.user_id AS business_id, u.name
```

---

## Complete Function Reference

### Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `count(expr)` | Count non-null values | `count(u)` |
| `count(*)` | Count all rows | `count(*)` |
| `count(DISTINCT expr)` | Count unique values | `count(DISTINCT u.country)` |
| `sum(expr)` | Sum numeric values | `sum(u.age)` |
| `avg(expr)` | Average of values | `avg(u.age)` |
| `min(expr)` | Minimum value | `min(u.age)` |
| `max(expr)` | Maximum value | `max(u.age)` |
| `collect(expr)` | Aggregate into list | `collect(u.name)` |
| `stDev(expr)` | Sample standard deviation | `stDev(u.age)` |
| `stDevP(expr)` | Population standard deviation | `stDevP(u.age)` |
| `percentileCont(expr, p)` | Continuous percentile | `percentileCont(u.age, 0.5)` |
| `percentileDisc(expr, p)` | Discrete percentile | `percentileDisc(u.age, 0.9)` |

### String Functions

| Function | Description | Example |
|----------|-------------|---------|
| `toLower(str)` | Convert to lowercase | `toLower('ABC')` → `'abc'` |
| `toUpper(str)` | Convert to uppercase | `toUpper('abc')` → `'ABC'` |
| `trim(str)` | Remove whitespace | `trim(' abc ')` → `'abc'` |
| `ltrim(str)` | Remove leading whitespace | `ltrim(' abc')` → `'abc'` |
| `rtrim(str)` | Remove trailing whitespace | `rtrim('abc ')` → `'abc'` |
| `substring(str, start, len)` | Extract substring | `substring('hello', 0, 3)` → `'hel'` |
| `left(str, n)` | Left n characters | `left('hello', 2)` → `'he'` |
| `right(str, n)` | Right n characters | `right('hello', 2)` → `'lo'` |
| `split(str, delim)` | Split into list | `split('a,b,c', ',')` → `['a','b','c']` |
| `replace(str, old, new)` | Replace substring | `replace('abc', 'b', 'x')` → `'axc'` |
| `reverse(str)` | Reverse string | `reverse('abc')` → `'cba'` |
| `size(str)` | String length | `size('hello')` → `5` |
| `startsWith(str, prefix)` | Check prefix | `startsWith('hello', 'he')` → `true` |
| `endsWith(str, suffix)` | Check suffix | `endsWith('hello', 'lo')` → `true` |
| `contains(str, sub)` | Check substring | `contains('hello', 'ell')` → `true` |
| `normalize(str)` | Unicode normalization | `normalize('café')` |
| `valueType(expr)` | Get value type name | `valueType(42)` → `'Int64'` |

### Date/Time Functions

| Function | Description | Example |
|----------|-------------|---------|
| `datetime()` | Current datetime | `datetime()` |
| `date()` | Current date | `date()` |
| `time()` | Current time | `time()` |
| `timestamp()` | Unix timestamp | `timestamp()` |
| `duration({...})` | Create duration | `duration({days: 7})` |
| `duration.between(d1, d2)` | Duration between dates | `duration.between(start, end)` |

### Mathematical Functions

| Function | Description | Example |
|----------|-------------|---------|
| `abs(n)` | Absolute value | `abs(-5)` → `5` |
| `sign(n)` | Sign (-1, 0, 1) | `sign(-5)` → `-1` |
| `round(n)` | Round to integer | `round(3.7)` → `4` |
| `ceil(n)` | Round up | `ceil(3.2)` → `4` |
| `floor(n)` | Round down | `floor(3.7)` → `3` |
| `sqrt(n)` | Square root | `sqrt(16)` → `4` |
| `pow(base, exp)` | Power | `pow(2, 3)` → `8` |
| `exp(n)` | Exponential (e^n) | `exp(1)` → `2.718...` |
| `log(n)` | Natural logarithm | `log(10)` |
| `log10(n)` | Base-10 logarithm | `log10(100)` → `2` |
| `rand()` | Random float [0, 1) | `rand()` |
| `pi()` | Pi constant | `pi()` → `3.14159...` |
| `e()` | Euler's number | `e()` → `2.71828...` |
| `sin(n)`, `cos(n)`, `tan(n)` | Trigonometric | `sin(3.14159)` |
| `asin(n)`, `acos(n)`, `atan(n)` | Inverse trigonometric | `asin(0.5)` |
| `atan2(y, x)` | Two-argument arctangent | `atan2(1, 1)` → `0.785...` |

### Date/Time Extraction Functions

| Function | Description | Example |
|----------|-------------|---------|
| `year(datetime)` | Extract year | `year(u.reg_date)` → `2024` |
| `month(datetime)` | Extract month (1-12) | `month(u.reg_date)` → `11` |
| `day(datetime)` | Extract day of month | `day(u.reg_date)` → `17` |
| `hour(datetime)` | Extract hour (0-23) | `hour(u.last_login)` → `14` |
| `minute(datetime)` | Extract minute | `minute(u.last_login)` → `30` |
| `second(datetime)` | Extract second | `second(u.last_login)` → `45` |
| `dayOfWeek(datetime)` | Day of week (1=Mon) | `dayOfWeek(u.reg_date)` → `3` |
| `dayOfYear(datetime)` | Day of year (1-366) | `dayOfYear(u.reg_date)` → `321` |
| `quarter(datetime)` | Quarter (1-4) | `quarter(u.reg_date)` → `4` |
| `week(datetime)` | ISO week number | `week(u.reg_date)` → `47` |
| `localdatetime()` | Current local datetime | `localdatetime()` |
| `localtime()` | Current local time | `localtime()` |

### Type Conversion Functions

| Function | Description | Example |
|----------|-------------|---------|
| `toInteger(expr)` | Convert to integer | `toInteger('42')` → `42` |
| `toFloat(expr)` | Convert to float | `toFloat('3.14')` → `3.14` |
| `toString(expr)` | Convert to string | `toString(42)` → `'42'` |
| `toBoolean(expr)` | Convert to boolean | `toBoolean('true')` → `true` |

### List Functions

| Function | Description | Example |
|----------|-------------|---------|
| `size(list)` | List length | `size([1,2,3])` → `3` |
| `head(list)` | First element | `head([1,2,3])` → `1` |
| `last(list)` | Last element | `last([1,2,3])` → `3` |
| `tail(list)` | All but first | `tail([1,2,3])` → `[2,3]` |
| `range(start, end)` | Generate range | `range(1, 5)` → `[1,2,3,4,5]` |
| `keys(map)` | Get map keys | `keys({a:1, b:2})` → `['a','b']` |
| `isEmpty(list)` | Check if empty | `isEmpty([])` → `true` |

### List Predicate Functions

| Function | Description | Example |
|----------|-------------|---------|
| `all(x IN list WHERE pred)` | All match predicate | `all(x IN [1,2,3] WHERE x > 0)` → `true` |
| `any(x IN list WHERE pred)` | Any match predicate | `any(x IN [1,2,3] WHERE x > 2)` → `true` |
| `none(x IN list WHERE pred)` | None match predicate | `none(x IN [1,2,3] WHERE x < 0)` → `true` |
| `single(x IN list WHERE pred)` | Exactly one matches | `single(x IN [1,2,3] WHERE x = 2)` → `true` |

### Path Functions

| Function | Description | Example |
|----------|-------------|---------|  
| `length(path)` | Number of edges | `length(path)` |
| `nodes(path)` | List of nodes | `nodes(path)` |
| `edges(path)` | List of edges | `edges(path)` |
| `shortestPath(...)` | Find shortest path | `shortestPath((a)-[*]-(b))` |
| `allShortestPaths(...)` | All shortest paths | `allShortestPaths((a)-[*]-(b))` |
| `cost(path)` | Total weight of weighted shortest path | `cost(path)` |

> **Note:** `cost(path)` requires a preceding WITH clause that exports exactly three aliases named `source`, `target`, and `weight`, representing edge weights. The query must also use `shortestPath()`. The weight CTE is automatically detected and used for weighted VLP traversal (Dijkstra-style `ORDER BY total_weight ASC` instead of `ORDER BY hop_count ASC`).

### Scalar Functions

| Function | Description | Example |
|----------|-------------|---------|  
| `COALESCE(expr1, ...)` | First non-null | `COALESCE(u.email, 'none')` |
| `nullIf(expr1, expr2)` | Return null if equal | `nullIf(u.status, 'unknown')` |
| `type(edge)` | Edge type | `type(e)` → `'FOLLOWS'` |
| `id(node)` | Node/edge ID | `id(u)` |

### Vector Similarity Functions

For similarity search on pre-computed embedding vectors (requires `Array(Float32)` columns):

| Function | Description | Example |
|----------|-------------|---------|
| `gds.similarity.cosine(v1, v2)` | Cosine similarity (0-1) | `gds.similarity.cosine(a.embedding, b.embedding)` |
| `gds.similarity.euclidean(v1, v2)` | Euclidean similarity (0-1) | `gds.similarity.euclidean(a.vec, b.vec)` |
| `gds.similarity.euclideanDistance(v1, v2)` | Raw Euclidean distance | `gds.similarity.euclideanDistance(a.vec, b.vec)` |
| `vector.similarity.cosine(v1, v2)` | Cosine similarity (Neo4j 5.x) | `vector.similarity.cosine(a.vec, b.vec)` |

#### Passing Vector Literals

For RAG (Retrieval-Augmented Generation) queries, pass pre-computed query embeddings as array literals:

```cypher
-- Vector literal syntax (array of floats)
MATCH (doc:Document)
RETURN doc.title, 
       gds.similarity.cosine(doc.embedding, [0.1, -0.2, 0.3, 0.15, -0.05]) AS similarity
ORDER BY similarity DESC
LIMIT 10

-- Using query parameters (recommended for production)
MATCH (doc:Document)
RETURN doc.title,
       gds.similarity.cosine(doc.embedding, $queryVector) AS similarity
ORDER BY similarity DESC
LIMIT 10
```

**HTTP API with vector parameter:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (doc:Document) RETURN doc.title, gds.similarity.cosine(doc.embedding, $vec) AS score ORDER BY score DESC LIMIT 5",
    "parameters": {
      "vec": [0.123, -0.456, 0.789, 0.012, -0.345]
    }
  }'
```

#### ClickHouse Index Requirements

For efficient vector search at scale, ClickHouse requires special indexes:

**HNSW Index (Approximate Nearest Neighbor):**
```sql
-- Create table with vector column
CREATE TABLE documents (
    id UInt64,
    title String,
    embedding Array(Float32),
    INDEX embedding_idx embedding TYPE vector_similarity('hnsw', 'cosineDistance')
) ENGINE = MergeTree() ORDER BY id;

-- Alternative: L2 distance
INDEX embedding_idx embedding TYPE vector_similarity('hnsw', 'L2Distance')
```

**Index Parameters:**
- `hnsw` - Hierarchical Navigable Small World algorithm
- `cosineDistance` or `L2Distance` - Distance metric
- Optional: `GRANULARITY` for index granularity

**Performance Notes:**
- Without index: Full table scan, O(n) - suitable for < 100K vectors
- With HNSW index: Approximate search, O(log n) - scales to millions of vectors
- HNSW returns approximate results (may miss some matches for speed)

**Important**: ClickHouse does NOT generate embeddings. Your application must:
1. Generate embeddings externally (OpenAI, Cohere, local models)
2. Store vectors in `Array(Float32)` columns
3. Pass query embeddings as parameters to ClickGraph

```cypher
-- RAG workflow example
-- Step 1: Your app calls OpenAI to embed "What is machine learning?"
-- Step 2: OpenAI returns [0.123, -0.456, ...] (1536 dimensions for ada-002)
-- Step 3: Pass to ClickGraph
MATCH (doc:Document)
WHERE doc.category = 'tech'
RETURN doc.title, doc.content,
       gds.similarity.cosine(doc.embedding, $queryEmbedding) AS relevance
ORDER BY relevance DESC
LIMIT 5
```

---

## ClickHouse Function Pass-Through

ClickGraph provides direct access to **any ClickHouse function** using the `ch.` and `chagg.` prefixes.

### Quick Reference

| Prefix | Use Case | GROUP BY |
|--------|----------|----------|
| `ch.` | Scalar functions + known aggregates (~150) | Auto for aggregates |
| `chagg.` | **Any** aggregate (explicit declaration) | Always generates |

```cypher
-- Scalar function (hash, JSON, URL, IP, geo, date...)
MATCH (u:User) RETURN ch.cityHash64(u.email) AS hash

-- Known aggregate (auto GROUP BY)
MATCH (u:User) RETURN u.country, ch.uniq(u.user_id) AS unique_users

-- Custom/new aggregate (explicit)
MATCH (u:User) RETURN u.country, chagg.myCustomAgg(u.score) AS result
```

### Supported Categories

| Category | Examples |
|----------|----------|
| **Hash** | `ch.cityHash64`, `ch.MD5`, `ch.SHA256` |
| **JSON** | `ch.JSONExtract*`, `ch.JSONHas` |
| **URL/IP** | `ch.domain`, `ch.IPv4NumToString` |
| **Geo** | `ch.greatCircleDistance`, `ch.geoToH3` |
| **Date** | `ch.toStartOfHour`, `ch.formatDateTime` |
| **Aggregates** | `ch.uniq`, `ch.quantile`, `ch.topK`, `ch.windowFunnel` |

📖 **[Full Documentation: ClickHouse Functions →](ClickHouse-Functions.md)**

The dedicated guide includes:
- Complete scalar function examples (hash, JSON, URL, IP, geo, date, string, array)
- All aggregate functions (unique counting, quantiles, TopK, funnel analysis, statistics)
- Function reference tables
- Limitations and workarounds

---

## Practice Exercises

### Exercise 1: Aggregations
```cypher
-- 1. Count users by country
-- 2. Find average age per country
-- 3. Find countries with more than 10 users
-- 4. Top 5 countries by user count
```

### Exercise 2: String Functions
```cypher
-- 1. Convert all names to uppercase
-- 2. Extract domain from email addresses
-- 3. Find users with names longer than 10 characters
-- 4. Create display name: "Name (Country)"
```

### Exercise 3: Date Functions
```cypher
-- 1. Find users registered in 2024
-- 2. Calculate account age in days
-- 3. Find users registered in last 30 days
-- 4. Group registrations by month
```

### Exercise 4: Complex Aggregations
```cypher
-- 1. Calculate follower-to-following ratio per user
-- 2. Find users with above-average follower counts
-- 3. Create age groups and count users in each
-- 4. Find most common interests (from lists)
```

**Solutions**: [Functions & Aggregations Solutions](Cypher-Functions-Solutions.md)

---

## Next Steps

You've mastered Cypher functions and aggregations! Continue learning:

- **[Advanced Patterns](Cypher-Advanced-Patterns.md)** - CASE, UNION, subqueries
- **[Optional Patterns](Cypher-Optional-Patterns.md)** - LEFT JOIN semantics
- **[Performance Tuning](Performance-Query-Optimization.md)** - Optimize queries

Or explore complete examples:
- **[Social Network Analysis](Use-Case-Social-Network.md)** - Friend recommendations
- **[Fraud Detection](Use-Case-Fraud-Detection.md)** - Transaction analysis

---

[← Back: Multi-Hop Traversals](Cypher-Multi-Hop-Traversals.md) | [Home](Home.md) | [Next: Advanced Patterns →](Cypher-Advanced-Patterns.md)
