package com.tavant.zomatoCapstone

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import problemStatements.{PBStmt1, PBStmt2, PBStmt3, PBStmt4 ,PBStmt5, PBStmt6}
import com.tavant.zomatoCapstone.Preprocessing.spark

object CapstoneMain {
  
  def main(args: Array[String]): Unit = {
    
//    System.setProperty("hadoop.home.dir", "***********/winutils-master/hadoop-2.7.1");
    
    val (dataframe_unused_columns, dataframe_used_columns) = Preprocessing.preprocess()
    dataframe_used_columns.persist()

    
//  Saving the two seperated DataFrames returned after preprocessing
    
    SaveAsParquet
    .savingAsParquetFile(dataframe_unused_columns, dataframe_used_columns, 
        "D:/BigData Training/Capstone Project/Preprocessing/UsedColumns", 
        "D:/BigData Training/Capstone Project/Preprocessing/UnusedColumns")  
    

//  Problem Statements
        
    val df = PBStmt1.probStatement01(dataframe_used_columns)
    df.persist()
 
    PBStmt2.probStatement02(df)
    df.unpersist()
   
    PBStmt3.probStatement03(dataframe_used_columns)
    
    PBStmt4.probStatement04(dataframe_used_columns)
      
    PBStmt5.probStatement05(dataframe_used_columns)
    
    PBStmt6.probStatement06(dataframe_used_columns)
  
    spark.close
  }
}