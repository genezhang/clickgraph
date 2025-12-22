-- Small OnTime Flights Test Data
-- Inserts minimal sample data into existing flights table for testing

-- Insert sample flight data (using only key columns)
-- LAX -> JFK flights
INSERT INTO default.flights (
    Year, Quarter, Month, DayofMonth, DayOfWeek, FlightDate,
    IATA_CODE_Reporting_Airline, Flight_Number_Reporting_Airline, Tail_Number,
    OriginAirportID, Origin, OriginCityName, OriginState, OriginWac,
    DestAirportID, Dest, DestCityName, DestState, DestWac,
    DepTime, DepDelay, ArrTime, ArrDelay, Distance
) VALUES
(2023, 1, 1, 15, 7, '2023-01-15', 'AA', 'AA100', 'N123AA', 12892, 'LAX', 'Los Angeles', 'CA', 91, 12478, 'JFK', 'New York', 'NY', 22, 805, 5, 1640, 10, 2475),
(2023, 1, 1, 16, 1, '2023-01-16', 'AA', 'AA100', 'N123AA', 12892, 'LAX', 'Los Angeles', 'CA', 91, 12478, 'JFK', 'New York', 'NY', 22, 758, -2, 1625, -5, 2475),
(2023, 1, 2, 20, 1, '2023-02-20', 'AA', 'AA100', 'N124AA', 12892, 'LAX', 'Los Angeles', 'CA', 91, 12478, 'JFK', 'New York', 'NY', 22, 815, 15, 1655, 25, 2475);

-- JFK -> LAX flights  
INSERT INTO default.flights (
    Year, Quarter, Month, DayofMonth, DayOfWeek, FlightDate,
    IATA_CODE_Reporting_Airline, Flight_Number_Reporting_Airline, Tail_Number,
    OriginAirportID, Origin, OriginCityName, OriginState, OriginWac,
    DestAirportID, Dest, DestCityName, DestState, DestWac,
    DepTime, DepDelay, ArrTime, ArrDelay, Distance
) VALUES
(2023, 1, 1, 15, 7, '2023-01-15', 'AA', 'AA200', 'N125AA', 12478, 'JFK', 'New York', 'NY', 22, 12892, 'LAX', 'Los Angeles', 'CA', 91, 905, 5, 1240, 10, 2475),
(2023, 1, 1, 16, 1, '2023-01-16', 'AA', 'AA200', 'N125AA', 12478, 'JFK', 'New York', 'NY', 22, 12892, 'LAX', 'Los Angeles', 'CA', 91, 855, -5, 1220, -10, 2475);

-- ORD -> LAX flights
INSERT INTO default.flights (
    Year, Quarter, Month, DayofMonth, DayOfWeek, FlightDate,
    IATA_CODE_Reporting_Airline, Flight_Number_Reporting_Airline, Tail_Number,
    OriginAirportID, Origin, OriginCityName, OriginState, OriginWac,
    DestAirportID, Dest, DestCityName, DestState, DestWac,
    DepTime, DepDelay, ArrTime, ArrDelay, Distance
) VALUES
(2023, 1, 3, 10, 5, '2023-03-10', 'UA', 'UA300', 'N200UA', 13930, 'ORD', 'Chicago', 'IL', 41, 12892, 'LAX', 'Los Angeles', 'CA', 91, 1005, 5, 1325, 10, 1745),
(2023, 1, 3, 11, 6, '2023-03-11', 'UA', 'UA300', 'N201UA', 13930, 'ORD', 'Chicago', 'IL', 41, 12892, 'LAX', 'Los Angeles', 'CA', 91, 958, -2, 1310, -5, 1745);

-- LAX -> ORD flights
INSERT INTO default.flights (
    Year, Quarter, Month, DayofMonth, DayOfWeek, FlightDate,
    IATA_CODE_Reporting_Airline, Flight_Number_Reporting_Airline, Tail_Number,
    OriginAirportID, Origin, OriginCityName, OriginState, OriginWac,
    DestAirportID, Dest, DestCityName, DestState, DestWac,
    DepTime, DepDelay, ArrTime, ArrDelay, Distance
) VALUES
(2023, 1, 3, 10, 5, '2023-03-10', 'UA', 'UA400', 'N202UA', 12892, 'LAX', 'Los Angeles', 'CA', 91, 13930, 'ORD', 'Chicago', 'IL', 41, 1410, 10, 2055, 15, 1745),
(2023, 2, 4, 5, 3, '2023-04-05', 'UA', 'UA400', 'N203UA', 12892, 'LAX', 'Los Angeles', 'CA', 91, 13930, 'ORD', 'Chicago', 'IL', 41, 1355, -5, 2030, -10, 1745);

-- JFK -> ORD flights
INSERT INTO default.flights (
    Year, Quarter, Month, DayofMonth, DayOfWeek, FlightDate,
    IATA_CODE_Reporting_Airline, Flight_Number_Reporting_Airline, Tail_Number,
    OriginAirportID, Origin, OriginCityName, OriginState, OriginWac,
    DestAirportID, Dest, DestCityName, DestState, DestWac,
    DepTime, DepDelay, ArrTime, ArrDelay, Distance
) VALUES
(2023, 2, 4, 15, 6, '2023-04-15', 'DL', 'DL500', 'N300DL', 12478, 'JFK', 'New York', 'NY', 22, 13930, 'ORD', 'Chicago', 'IL', 41, 705, 5, 925, 10, 740);

-- Verification
SELECT 'Flights loaded:' AS status, count(*) AS count FROM default.flights;
SELECT 'Airports (distinct origins):' AS status, count(DISTINCT OriginAirportID) AS count FROM default.flights;
SELECT 'Airports (distinct destinations):' AS status, count(DISTINCT DestAirportID) AS count FROM default.flights;
SELECT 'Sample data:' AS status;
SELECT FlightDate, IATA_CODE_Reporting_Airline, Origin, Dest, DepDelay, ArrDelay 
FROM default.flights 
ORDER BY FlightDate 
LIMIT 5;
