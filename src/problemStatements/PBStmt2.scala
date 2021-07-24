package problemStatements

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.desc
import com.tavant.zomatoCapstone.SaveAsParquet
import com.tavant.zomatoCapstone.Preprocessing.spark

object PBStmt2 {

/**
 * Problem Statement 2 :
 * Group by Address location for the closed restaurants and find out which area has the most restaurants getting closed
 */
    
  def probStatement02(df: DataFrame) = {
    
    val requiredDF = df.groupBy("location_new").count().sort(desc("count"))
    requiredDF.persist()
    
    //  Answer:- South Bangalore(42)
    println("The area where most restaurants got closed is " + requiredDF.first().getString(0))
    println("The number of restaurants closed in " + requiredDF.first().getString(0)
        + " are " + requiredDF.first().getLong(1))
        
//  Setting the number of partitions
    var resDF1:DataFrame = spark.emptyDataFrame
    if (requiredDF.rdd.getNumPartitions > 4)
      {resDF1 = requiredDF.coalesce(4)}
    else if (requiredDF.rdd.getNumPartitions < 4)
      {resDF1 = requiredDF.repartition(4)}
    else
      {resDF1 = requiredDF}
    
    SaveAsParquet.savingAsParquetFile(resDF1, "D:/BigData Training/Capstone Project/Problem02")
    
    requiredDF.unpersist()
  }
}