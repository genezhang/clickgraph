SELECT 
      count(r.follower_id) AS `total_follows`
FROM test_integration.follows AS r
