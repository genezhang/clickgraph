SELECT 
      count(DISTINCT u.exposure) AS "unique_exposures"
FROM data_security.ds_users AS u
