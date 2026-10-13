SELECT 
      count(CASE WHEN v0.age < 30 THEN 1 END) AS "young", 
      count(CASE WHEN v0.age >= 30 THEN 1 END) AS "mature"
FROM test_integration.users AS v0

-- column young: Value [("young", "young")]
-- column mature: Value [("mature", "mature")]