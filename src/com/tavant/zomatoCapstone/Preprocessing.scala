package com.tavant.zomatoCapstone

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions.{col, split, element_at}
import requiredUDFs.{RemoveNonAsciiUDF, MenuListUDF, GetRatingsUDF}

object Preprocessing {
    val spark = SparkSession.builder()
    //.master("local[*]")
    .getOrCreate()
    
  def preprocess():Tuple2[DataFrame,DataFrame] = {

//  Reading the CSV file
    val zomatodf = spark.read
    .option("header", "true")
    .option("multiline", "true")
    .option("escape","\"")
    .format("csv")
    //.load("D:/BigData Training/Capstone Project/zomato.csv")
    .load("s3://ravi-zomato-capstone/zomato-dataset/zomato.csv")
    
    
//  Splitting the columns on the basis of some delimiter and storing it in a new column
    val zdf1 = zomatodf.withColumn("phone_new", split(zomatodf("phone"),"\\R")).drop("phone")
    .withColumn("rest_type_new", split(zomatodf("rest_type"),",")).drop("rest_type")
    .withColumn("dish_liked_new", split(zomatodf("dish_liked"),",")).drop("dish_liked")
    .withColumn("cuisines_new", split(zomatodf("cuisines"),",")).drop("cuisines")
    
//  Casting votes and approx_cost column to Integer type
    .withColumn("votes_new", zomatodf("votes").cast("Integer")).drop("votes")
    .withColumn("approx_cost_new", zomatodf("approx_cost(for two people)").cast("Integer")).drop("approx_cost(for two people)")
    
//  Splitting and converting rate column to double as all the rating is out of 5
    .withColumn("rating", element_at(split(zomatodf("rate"),"/"),1).cast("Double")).drop("rate")


//  Removing rows with null Rating
    .filter(col("rating").isNotNull)


//  Required UDFs
    val menu_item_to_array = MenuListUDF.menu_list_udf()
    val removeNonAscii = RemoveNonAsciiUDF.removeNonAscii_udf()
    val get_ratings = GetRatingsUDF.getRatings_udf()

    
//  Creating Array out of elements of menu_item and storing it in new column  
    val zdf2 = zdf1.withColumn("menu_item_new", menu_item_to_array(col("menu_item"))).drop("menu_item")

    
//  Removing non-ASCII from columns and converting reviews_list to Array[(String,String)]
    .withColumn("name_new", removeNonAscii(col("name"))).drop("name")
    .withColumn("location_new", removeNonAscii(col("location"))).drop("location")
    .withColumn("address_new", removeNonAscii(col("address"))).drop("address")
    .withColumn("reviews_list_new", removeNonAscii(col("reviews_list"))).drop("reviews_list")
    .withColumn("review_rating", get_ratings(col("reviews_list_new")))

    
//  Renaming listed_in(city) and listed_in(type) column
    .withColumnRenamed("listed_in(city)", "listedInCity")
    .withColumnRenamed("listed_in(type)", "listedInType")
    
    
//  Adding index to the rows and rearranging them
    val zdf3 = AddingIndex.dfZipWithIndex(zdf2)
    

//  Creating temp views
    zdf3.createOrReplaceTempView("zomatotable")
    
    
//  Separating the dataFrame into 2 parts for fast performance
    val df_unused_columns = spark.sql("select 'id', 'address_new', 'online_order', 'book_table', " +
                            "'reviews_list_new', 'votes_new', 'phone_new', 'dish_liked_new', " +
                            "'menu_item_new', 'listedInCity', 'listedInType' from zomatotable")
    
    val df_used_columns = zdf3.select(col("id"), col("url"), col("name_new"), col("rating"), 
        col("location_new"), col("rest_type_new"),
        col("cuisines_new"), col("approx_cost_new"), col("review_rating"))
  

//  Setting the number of partitions for df_unused_columns
    var df_unused:DataFrame = spark.emptyDataFrame
    if (df_unused_columns.rdd.getNumPartitions > 4)
      {df_unused = df_unused_columns.coalesce(4)}
    else if (df_unused_columns.rdd.getNumPartitions < 4)
      {df_unused = df_unused_columns.repartition(4)}
    else
      {df_unused = df_unused_columns}
    
    
//  Setting the number of partitions for df_used_columns
    var df_used:DataFrame = spark.emptyDataFrame
    if (df_used_columns.rdd.getNumPartitions > 4)
      {df_used = df_used_columns.coalesce(4)}
    else if (df_used_columns.rdd.getNumPartitions < 4)
      {df_used = df_used_columns.repartition(4)}
    else
      {df_used = df_used_columns}
    
    
    
//  Returning the Tuple2 of both the dataFrames
    (df_unused, df_used)
       
    
  }
}