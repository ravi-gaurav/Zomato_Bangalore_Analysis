package problemStatements

import org.apache.spark.sql.DataFrame
import com.tavant.zomatoCapstone.SaveAsParquet
import com.tavant.zomatoCapstone.Preprocessing.spark

object PBStmt1 {

/**
 * Problem Statement 1 :
 * Filter out records which have invalid restaurant links, which means that the restaurant is most probably closed now
 */
    
  def probStatement01(df: DataFrame):DataFrame = {
    
    val requiredDF = df.filter(!df("url").startsWith("https://www.zomato.com/bangalore/"))
    
//  Setting the number of partitions
    var resDF1:DataFrame = spark.emptyDataFrame
    if (requiredDF.rdd.getNumPartitions > 4)
      {resDF1 = requiredDF.coalesce(4)}
    else if (requiredDF.rdd.getNumPartitions < 4)
      {resDF1 = requiredDF.repartition(4)}
    else
      {resDF1 = requiredDF}
    
    SaveAsParquet.savingAsParquetFile(resDF1, "D:/BigData Training/Capstone Project/Problem01")
    
    resDF1
    
  }
}