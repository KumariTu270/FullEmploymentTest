#This is git test
package com.hashmap

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Utils {
  val removeCode:UserDefinedFunction = udf((locality: String) => {
    if (locality != null && locality.contains("(")) {
      val name = locality.split("\\(")
      name(0)
    }
    else if (!locality.contains("(")) {
      locality
    }
    esle{
      null
    }
  })


}
