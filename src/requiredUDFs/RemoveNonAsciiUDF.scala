package requiredUDFs

import org.apache.spark.sql.functions.udf

object RemoveNonAsciiUDF {
  
//  UDF to remove non-ASCII characters from a column and also make the content more readable
//  by removing some other unwanted characters from string   
    def removeNonAscii_udf() = {
      val nonAscii_udf = (x:String) => {
        val str = x.replaceAll("[^\\x00-\\x7F]", "")
        .replaceAll("\\\\n","")
        .replaceAll("\\\\x[0-9]{2}", "")
        .replaceAll("Rated ", "")
        .replaceAll("RATED ", "")
        .replaceAll("[?]", "")
        .trim
        
        str
      }
      
      val removeNonAscii = udf(nonAscii_udf)
      removeNonAscii
    }
}