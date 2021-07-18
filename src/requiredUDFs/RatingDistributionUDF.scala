package requiredUDFs

import org.apache.spark.sql.functions.udf
import java.text.DecimalFormat

object RatingDistributionUDF {
  
  def ratingDist_UDF() = {
    
//  UDF to get the percentage distribution of review_rating
    val rating_distribution_udf = (arr:Seq[Integer]) => {
      val count = arr.size
      val res = arr.groupBy(identity).mapValues(x=> 
        {  
          val temp = (x.size.floatValue()/count)*100
          val dec = new DecimalFormat("#0.00");
          dec.format(temp)      
        })
      res
    }
    
    val rating_distribution = udf(rating_distribution_udf)
    rating_distribution
  }
}