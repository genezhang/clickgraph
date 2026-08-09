SELECT 
      if(match(toString(POWER(2, 3)), '^-?[0-9]+$'), concat(toString(POWER(2, 3)), '.0'), toString(POWER(2, 3))) AS "s"
