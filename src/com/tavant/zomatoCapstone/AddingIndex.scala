package com.tavant.zomatoCapstone

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object AddingIndex {
  
  def dfZipWithIndex(df: DataFrame, offset: Int = 1, colName: String = "id", inFront: Boolean = true): DataFrame = {
    
      df.sqlContext.createDataFrame(
        df.rdd.zipWithIndex.map(ln =>
            Row.fromSeq(
                (if (inFront) Seq(ln._2 + offset) else Seq())
                ++ ln._1.toSeq ++
                (if (inFront) Seq() else Seq(ln._2 + offset))
            )
        ),
        StructType(
            (if (inFront) Array(StructField(colName,LongType,false)) else Array[StructField]()) 
            ++ df.schema.fields ++ 
            (if (inFront) Array[StructField]() else Array(StructField(colName,LongType,false)))
        )
      )
   }
}