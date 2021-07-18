package requiredUDFs

import org.apache.spark.sql.functions.udf

object GetRatingsUDF {
  
    def getRatings_udf() = {
      
//  UDF for finding all the rating among the review_list and converting it to float and storing it in an Sequence
      val get_ratings_udf = (str:String) => {
        val pattern = "[0-9]\\.[0-9]".r
        val arr = pattern.findAllIn(str).toSeq
        .map(x => x.toFloat.round)
        .filter(x => x<=5 && x>=1)
        
        arr
      }
    
      val get_ratings = udf(get_ratings_udf)
      get_ratings
  }
}