package com.RUSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

/* any necessary Java packages here */
/**
 * 1- Read the reddit data
 * 2- Define a schema for the data that is given
 * 3- Calculate the impact of each image
 * 4- Save the results
 */
public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
    // 1
		SparkSession spark = SparkSession.builder().appName("Reddit").getOrCreate();
		Dataset<Row> redditData = spark.read().option("header",true).csv("../../../../../../RedditData/RedditData-Small.csv");
    
    // 2
    StructType schema = new StructType().add("image_id", DataTypes.LongType, true)
    .add("unixtime", DataTypes.LongType, true)
    .add("post_title", DataTypes.StringType, true)
    .add("subreddit", DataTypes.StringType, true)
    .add("upvotes", DataTypes.IntegerType, true)
    .add("downvotes", DataTypes.IntegerType, true)
    .add("comments", DataTypes.IntegerType, true);

    // 3
    spark.udf().register("calculateImpact", (Integer upvotes, Integer downvotes, Integer comments) -> upvotes + downvotes + comments, DataTypes.IntegerType);

    redditData = redditData.withColumn("impact", callUDF("calculateImpact", col("upvotes"), col("downvotes"), col("comments")));
    Dataset<Row> imageImpact = redditData.groupBy("image_id").agg(sum("impact").alias("total_impact"));

    // 4
    imageImpact.write().csv("/src/main/java/com/RUSpark");
    


	}

}
