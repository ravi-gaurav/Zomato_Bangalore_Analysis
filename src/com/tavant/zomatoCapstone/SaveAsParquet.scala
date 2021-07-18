package com.tavant.zomatoCapstone

import org.apache.spark.sql.{DataFrame,SaveMode}

object SaveAsParquet {
 
//  The method savingAsParquetFile is overloaded
  
    def savingAsParquetFile(df1:DataFrame, df2:DataFrame, path1:String, path2:String) = {
      df1.write.mode(SaveMode.Overwrite).parquet(path1)
      df2.write.mode(SaveMode.Overwrite).parquet(path2)
    }
  
    def savingAsParquetFile(df:DataFrame, path:String) = {
      df.write.mode(SaveMode.Overwrite).parquet(path)
    }
  
    def savingAsParquetFile(df:DataFrame, path:String, partitionBy:String) = {
      df.write.mode(SaveMode.Overwrite).partitionBy(partitionBy).parquet(path)
    }
}