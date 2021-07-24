package problemStatements

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,size}
import requiredUDFs.RatingDistributionUDF
import com.tavant.zomatoCapstone.SaveAsParquet
import com.tavant.zomatoCapstone.Preprocessing.spark

object PBStmt4 {
   
/**
 * Problem Statement 4 :
 * For the reviews list column, find the distribution of star rating,
 * on the condition that there are -at-least 30 ratings for that restaurant
 */
    
    def probStatement04(df: DataFrame) = {
      
      val rating_distribution = RatingDistributionUDF.ratingDist_UDF()

//  Filter review_rating with atleast 30 ratings
      val resDF1 = df.filter(size(col("review_rating")) >= 30)
      
//  Finding the distribution of star rating in percentage
      .withColumn("rating_distribution", rating_distribution(col("review_rating")))
      .select(col("id"), col("name_new"), col("location_new"), col("rating_distribution"))
      
//  Setting the number of partitions
      var resDF2:DataFrame = spark.emptyDataFrame
      if (resDF1.rdd.getNumPartitions > 4)
        {resDF2 = resDF1.coalesce(4)}
      else if (resDF1.rdd.getNumPartitions < 4)
        {resDF2 = resDF1.repartition(4)}
      else
        {resDF2 = resDF1}
      
      SaveAsParquet.savingAsParquetFile(resDF2, "D:/BigData Training/Capstone Project/Problem04")
      
  }
}