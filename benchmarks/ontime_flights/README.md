# OnTime Flights Benchmark

This benchmark uses the OnTime flight data from ClickHouse's example datasets to test graph queries on denormalized edge tables.

## Data Source

The OnTime dataset contains US domestic flight data with detailed information about flight times, delays, and airport information.

**Source**: https://clickhouse.com/docs/getting-started/example-datasets/ontime

## Data Setup

### 1. Create the Table

```sql
CREATE TABLE ontime
(
    Year                            UInt16,
    Quarter                         UInt8,
    Month                           UInt8,
    DayofMonth                      UInt8,
    DayOfWeek                       UInt8,
    FlightDate                      Date,
    Reporting_Airline               LowCardinality(String),
    DOT_ID_Reporting_Airline        Int32,
    IATA_CODE_Reporting_Airline     LowCardinality(String),
    Tail_Number                     LowCardinality(String),
    Flight_Number_Reporting_Airline LowCardinality(String),
    OriginAirportID                 Int32,
    OriginAirportSeqID              Int32,
    OriginCityMarketID              Int32,
    Origin                          FixedString(5),
    OriginCityName                  LowCardinality(String),
    OriginState                     FixedString(2),
    OriginStateFips                 FixedString(2),
    OriginStateName                 LowCardinality(String),
    OriginWac                       Int32,
    DestAirportID                   Int32,
    DestAirportSeqID                Int32,
    DestCityMarketID                Int32,
    Dest                            FixedString(5),
    DestCityName                    LowCardinality(String),
    DestState                       FixedString(2),
    DestStateFips                   FixedString(2),
    DestStateName                   LowCardinality(String),
    DestWac                         Int32,
    CRSDepTime                      Int32,
    DepTime                         Int32,
    DepDelay                        Int32,
    DepDelayMinutes                 Int32,
    DepDel15                        Int32,
    DepartureDelayGroups            LowCardinality(String),
    DepTimeBlk                      LowCardinality(String),
    TaxiOut                         Int32,
    WheelsOff                       LowCardinality(String),
    WheelsOn                        LowCardinality(String),
    TaxiIn                          Int32,
    CRSArrTime                      Int32,
    ArrTime                         Int32,
    ArrDelay                        Int32,
    ArrDelayMinutes                 Int32,
    ArrDel15                        Int32,
    ArrivalDelayGroups              LowCardinality(String),
    ArrTimeBlk                      LowCardinality(String),
    Cancelled                       Int8,
    CancellationCode                FixedString(1),
    Diverted                        Int8,
    CRSElapsedTime                  Int32,
    ActualElapsedTime               Int32,
    AirTime                         Int32,
    Flights                         Int32,
    Distance                        Int32,
    DistanceGroup                   Int8,
    CarrierDelay                    Int32,
    WeatherDelay                    Int32,
    NASDelay                        Int32,
    SecurityDelay                   Int32,
    LateAircraftDelay               Int32,
    FirstDepTime                    Int32,
    TotalAddGTime                   Int32,
    LongestAddGTime                 Int32,
    DivAirportLandings              Int32,
    DivReachedDest                  Int32,
    DivActualElapsedTime            Int32,
    DivArrDelay                     Int32,
    DivDistance                     Int32,
    Div1Airport                     LowCardinality(String),
    Div1AirportID                   Int32,
    Div1AirportSeqID                Int32,
    Div1WheelsOn                    LowCardinality(String),
    Div1TotalGTime                  Int32,
    Div1LongestGTime                Int32,
    Div1WheelsOff                   LowCardinality(String),
    Div1TailNum                     LowCardinality(String),
    Div2Airport                     LowCardinality(String),
    Div2AirportID                   Int32,
    Div2AirportSeqID                Int32,
    Div2WheelsOn                    LowCardinality(String),
    Div2TotalGTime                  Int32,
    Div2LongestGTime                Int32,
    Div2WheelsOff                   LowCardinality(String),
    Div2TailNum                     LowCardinality(String),
    Div3Airport                     LowCardinality(String),
    Div3AirportID                   Int32,
    Div3AirportSeqID                Int32,
    Div3WheelsOn                    LowCardinality(String),
    Div3TotalGTime                  Int32,
    Div3LongestGTime                Int32,
    Div3WheelsOff                   LowCardinality(String),
    Div3TailNum                     LowCardinality(String),
    Div4Airport                     LowCardinality(String),
    Div4AirportID                   Int32,
    Div4AirportSeqID                Int32,
    Div4WheelsOn                    LowCardinality(String),
    Div4TotalGTime                  Int32,
    Div4LongestGTime                Int32,
    Div4WheelsOff                   LowCardinality(String),
    Div4TailNum                     LowCardinality(String),
    Div5Airport                     LowCardinality(String),
    Div5AirportID                   Int32,
    Div5AirportSeqID                Int32,
    Div5WheelsOn                    LowCardinality(String),
    Div5TotalGTime                  Int32,
    Div5LongestGTime                Int32,
    Div5WheelsOff                   LowCardinality(String),
    Div5TailNum                     LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (Year, Quarter, Month, DayofMonth, FlightDate, IATA_CODE_Reporting_Airline, Flight_Number_Reporting_Airline);
```

### 2. Download and Load Data

```bash
# Download data files (2021-2023)
for year in {2021..2023}; do
    for month in {1..12}; do
        url="https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${year}_${month}.zip"
        wget --continue --tries=3 "$url" -O "data_${year}_${month}.zip" || true
    done
done

# Load into ClickHouse
# Note: Using -pq instead of -cq (unzip -c not available on all systems)
ls -1 *.zip | xargs -I{} -P $(nproc) bash -c "echo {}; unzip -pq {} '*.csv' | sed 's/\.00//g' | clickhouse-client --input_format_csv_empty_as_default 1 --query='INSERT INTO ontime FORMAT CSVWithNames'"
```

### 3. Verify Data

```sql
SELECT count() FROM ontime;
-- Should return ~60+ million rows for 2017-2024

SELECT min(FlightDate), max(FlightDate) FROM ontime;
-- Shows date range of loaded data
```

## Graph Schema

The OnTime data is modeled as a denormalized edge table:

- **Nodes**: `Airport` (denormalized - properties embedded in edge table)
- **Edges**: `FLIGHT` (each row is a flight connecting two airports)

See `schemas/ontime_benchmark.yaml` for the full schema definition.

## Graph Model

```
(origin:Airport)-[:FLIGHT]->(dest:Airport)
```

Where Airport properties (code, city, state) are denormalized on the FLIGHT edge table:
- Origin airport: `Origin`, `OriginCityName`, `OriginState`, etc.
- Destination airport: `Dest`, `DestCityName`, `DestState`, etc.

## Running Benchmarks

```bash
# Start ClickGraph server with ontime schema
export GRAPH_CONFIG_PATH="./benchmarks/ontime_flights/schemas/ontime_benchmark.yaml"
cargo run --release --bin clickgraph

# Run benchmark queries
cd benchmarks/ontime_flights/queries
python3 run_ontime_benchmark.py
```

## Example Queries

```cypher
-- Find all flights from LAX
MATCH (lax:Airport {code: 'LAX'})-[f:FLIGHT]->(dest:Airport)
RETURN dest.code, dest.city, count(f) as flights
ORDER BY flights DESC
LIMIT 10

-- Find routes with high delays
MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
WHERE f.ArrDelay > 60
RETURN origin.code, dest.code, avg(f.ArrDelay) as avg_delay
ORDER BY avg_delay DESC
LIMIT 20
```

## Notes

- This benchmark tests **denormalized edge table** handling
- Airport nodes are virtual (no physical airport table)
- All airport properties come from the edge table columns
- Good for testing property resolution in denormalized patterns
