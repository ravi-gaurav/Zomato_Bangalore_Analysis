package requiredUDFs

import org.apache.spark.sql.functions.udf

object CostBucketUDF {
  
  def cost_bucket_udf() = {
    val cost_bucketUDF = (cost: Integer) => {
      var cBucket = ""
      
      if(cost<=300)
        cBucket = "A"
      else if(cost>300 && cost<=500)
        cBucket = "B"
      else if(cost>500 && cost<800)
        cBucket = "C"
      else
        cBucket = "D"
        
      cBucket
    }
    
    val cost_bucket = udf(cost_bucketUDF)
    cost_bucket
    
  }
}