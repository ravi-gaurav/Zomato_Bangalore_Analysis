# Zomato_Bangalore_Analysis

## DATASET

1.  Download the Zomato-Bangalore Dataset from the below mentioned link :- https://www.kaggle.com/himanshupoddar/zomato-bangalore-restaurants
2.  The dataset needs preprocessing before you can start with analysis.



## PROJECT

1.  This project is in scala-spark
2.  6 Usecases solved->

      -  Filter out records which have invalid restaurant links (use regex to take main part of the url) – which means that the restaurant is most probably closed now.
      
      -  Group by Address location for the closed restaurants and find out which area has the most restaurants getting closed.
      
      -  For the restaurants which are still working, group by restaurant type and location and find out the restaurants which have highest rating for each cuisine type.
      
      -  For the reviews list column, find the distribution of star rating, on the condition that there are -at-least 30 ratings for that restaurant.
      
      -  Group by location for individual cost buckets (for 2 people) : [<=300, 300-500, 500-800, >= 800] and take the 5 highest rated restaurants in each location and each cost bucket and save as parquet file.
      
      -  For the restaurants which are not closed, publish the data into individual s3 partitions based on location, so that downstream processes can customize promotions based on this.



## ADDITIONAL INFO

1.  Total number of records-> 51717
2.  Number of records with null rating-> 10052
3.  Number of records with non-null rating-> 41665
4.  Size of data in review column is ~7 times the size of data in other columns combined
      
      
