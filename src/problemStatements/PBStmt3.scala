package problemStatements

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{explode, col, row_number, desc, trim}
import org.apache.spark.sql.expressions.Window
import com.tavant.zomatoCapstone.SaveAsParquet
import com.tavant.zomatoCapstone.Preprocessing.spark

object PBStmt3 {
   
/**
 * Problem Statement 3 :
 * For the restaurants which are still working, group by restaurant type and location
 * and find out the restaurants which have highest rating for each cuisine type
 */
    
  def probStatement03(df: DataFrame) = {
    
    val workingRestaurants = df.filter(df("url").startsWith("https://www.zomato.com/bangalore/"))
    
//  Exploding cuisine_new column
    val resDF1 = workingRestaurants.select(col("id"), col("name_new"), col("rest_type_new"),
        col("location_new"), explode(col("cuisines_new")).as("cuisine"), col("rating"))
     
//  Exploding rest_type_new column
    .select(col("id"), col("name_new"), explode(col("rest_type_new")).as("rest_type"),
        col("location_new"), col("cuisine"), col("rating"))

//  Trimming the string in rest_type and cuisine column for leading and extra spaces
    .withColumn("restaurant_type", trim(col("rest_type"))).drop("rest_type")
    .withColumn("cuisine_type", trim(col("cuisine"))).drop("cuisine")
  

    val winSpec = Window.partitionBy("restaurant_type", "location_new", "cuisine_type").orderBy(col("rating").desc)
    
    val resDF2 = resDF1.withColumn("ranking", row_number.over(winSpec))
    .filter(col("ranking") === 1)
    .select(col("restaurant_type"), col("location_new"), col("cuisine_type"), col("id"), col("name_new"))
    
//  Setting the number of partitions
    var resDF3:DataFrame = spark.emptyDataFrame
    if (resDF2.rdd.getNumPartitions > 4)
      {resDF3 = resDF2.coalesce(4)}
    else if (resDF2.rdd.getNumPartitions < 4)
      {resDF3 = resDF2.repartition(4)}
    else
      {resDF3 = resDF2}
      
    SaveAsParquet.savingAsParquetFile(resDF3, "D:/BigData Training/Capstone Project/Problem03")
    
  }
}