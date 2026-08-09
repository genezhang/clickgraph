SELECT 
      if(match(toString(toFloat64(3)), '^-?[0-9]+$'), concat(toString(toFloat64(3)), '.0'), toString(toFloat64(3))) AS "s"
