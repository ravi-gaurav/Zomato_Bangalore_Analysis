package problemStatements

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, row_number}
import requiredUDFs.CostBucketUDF
import org.apache.spark.sql.expressions.Window
import com.tavant.zomatoCapstone.SaveAsParquet
import com.tavant.zomatoCapstone.Preprocessing.spark

object PBStmt5 {
    
/**
 * Problem Statement 5 :
 * Group by location for individual cost buckets (for 2 people) : [<=300, 300-500, 500-800, >= 800]
 * and take the 5 highest rated restaurants in each location and each cost bucket and save as parquet file
 */
    
    def probStatement05(df: DataFrame) = {
      
//  Cost Bucket is as follows
//  A => cost<=300
//  B => cost>300 && cost<=500
//  C => cost>500 && cost<800
//  D => cost>=800
      
      val cost_bucket = CostBucketUDF.cost_bucket_udf()
      
      val resDF1 = df
      .filter(col("approx_cost_new").isNotNull)
      .withColumn("cost_bucket", cost_bucket(col("approx_cost_new")))
      
      val winSpec = Window.partitionBy("location_new", "cost_bucket").orderBy(col("rating").desc)
      
      val resDF2 = resDF1.withColumn("ranking", row_number.over(winSpec))
      .filter(col("ranking")<=5)
      .select(col("location_new"), col("cost_bucket"), col("id"), col("name_new"), col("rating"))
      
//  Setting the number of partitions
      var resDF3:DataFrame = spark.emptyDataFrame
      if (resDF2.rdd.getNumPartitions > 4)
        {resDF3 = resDF2.coalesce(4)}
      else if (resDF2.rdd.getNumPartitions < 4)
        {resDF3 = resDF2.repartition(4)}
      else
        {resDF3 = resDF2}
      
      //SaveAsParquet.savingAsParquetFile(resDF3, "D:/BigData Training/Capstone Project/Problem05")
      SaveAsParquet.savingAsParquetFile(resDF3, "s3://ravi-zomato-capstone/output/problem05")
  }
}