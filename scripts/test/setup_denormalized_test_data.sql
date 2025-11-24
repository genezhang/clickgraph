-- Setup script for denormalized edge table integration tests
-- Creates flights table with denormalized airport properties (OnTime-style)
-- Note: No separate airports table - Airport nodes are virtual, derived from flights table

-- Create denormalized flights table
-- This is the key table: it contains BOTH edge data AND denormalized node properties
CREATE TABLE IF NOT EXISTS test_integration.flights (
    flight_id UInt32,
    flight_number String,
    airline String,
    
    -- Origin airport (from_node) - denormalized properties
    origin_code String,
    origin_city String,
    origin_state String,
    
    -- Destination airport (to_node) - denormalized properties
    dest_code String,
    dest_city String,
    dest_state String,
    
    -- Flight properties
    dep_time String,
    arr_time String,
    distance_miles UInt32
) ENGINE = Memory;

-- Insert denormalized flight data
-- Notice: Airport properties are DUPLICATED in each flight row
INSERT INTO test_integration.flights VALUES
    -- LAX -> SFO
    (1, 'AA100', 'American Airlines', 
     'LAX', 'Los Angeles', 'CA',
     'SFO', 'San Francisco', 'CA',
     '08:00', '09:30', 337),
    
    -- SFO -> JFK
    (2, 'UA200', 'United Airlines',
     'SFO', 'San Francisco', 'CA',
     'JFK', 'New York', 'NY',
     '10:00', '18:30', 2586),
    
    -- JFK -> LAX
    (3, 'DL300', 'Delta Airlines',
     'JFK', 'New York', 'NY',
     'LAX', 'Los Angeles', 'CA',
     '09:00', '12:30', 2475),
    
    -- ORD -> ATL
    (4, 'AA400', 'American Airlines',
     'ORD', 'Chicago', 'IL',
     'ATL', 'Atlanta', 'GA',
     '07:00', '10:00', 606),
    
    -- ATL -> LAX
    (5, 'DL500', 'Delta Airlines',
     'ATL', 'Atlanta', 'GA',
     'LAX', 'Los Angeles', 'CA',
     '11:00', '13:30', 1946),
    
    -- LAX -> ORD (connecting flight)
    (6, 'UA600', 'United Airlines',
     'LAX', 'Los Angeles', 'CA',
     'ORD', 'Chicago', 'IL',
     '14:00', '20:00', 1745);
