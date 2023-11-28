package com.RUSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/* any necessary Java packages here */
/**
1- Initialize spark session
2- Read input CSV data into a DataFrame
3- Calculate the average rating for each movie
4- Format the output to truncate average rating to 2 decimal places
5- Output the results
6- Stop the spark session
 */
public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
    // 1
    SparkSession spark = SparkSession.builder().appName("AverageRating").getOrCreate();

    // 2
    Dataset<Row> reviewsData = spark.read().csv(InputPath)
            .toDF("movie_id", "customer_id", "rating", "date");

    // 3
    Dataset<Row> avgRatings = reviewsData.groupBy("movie_id")
            .agg(functions.avg("rating").alias("average_rating"));

    // 4
    avgRatings = avgRatings.withColumn("average_rating", functions.format_number(avgRatings.col("average_rating"), 2));

    // 5
    avgRatings.select("movie_id", "average_rating").show();
    
    // 6
    spark.stop();
	}
}
