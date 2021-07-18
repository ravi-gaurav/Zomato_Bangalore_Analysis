package problemStatements

import org.apache.spark.sql.DataFrame
import com.tavant.zomatoCapstone.SaveAsParquet
import com.tavant.zomatoCapstone.Preprocessing.spark

object PBStmt6 {
   
/**
 * Problem Statement 6 :
 * For the restaurants which are not closed, publish the data into individual s3 partitions based on location,
 * so that downstream processes can customize promotions based on this
 */
    
    def probStatement06(df: DataFrame):Unit = {
      
      val workingRestaurants = df.filter(df("url").startsWith("https://www.zomato.com/bangalore/"))
      
//  Setting the number of partitions
      var resDF1:DataFrame = spark.emptyDataFrame
      if (workingRestaurants.rdd.getNumPartitions > 4)
        {resDF1 = workingRestaurants.coalesce(4)}
      else if (workingRestaurants.rdd.getNumPartitions < 4)
        {resDF1 = workingRestaurants.repartition(4)}
      else
        {resDF1 = workingRestaurants}
      
      
      //SaveAsParquet.savingAsParquetFile(resDF1, "D:/BigData Training/Capstone Project/Problem06", "location_new")
      SaveAsParquet.savingAsParquetFile(resDF1, "s3://ravi-zomato-capstone/output/problem06", "location_new")

    
  }
}