package com.RUSpark;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/* any necessary Java packages here */

/**
1- Start the spark session
2- Define the schema for the input data
3- Read the input CSV data with the specified schema
4- Convert UNIX timestamps to the EST timezone
5- Extract the hour from the converted timestamps
6- Calculate the impact score for each hour
7- Aggregate the impact scores for each hour
8- Output the results in the specified format

 */

public class RedditHourImpact {

	public static void main(String[] args) throws Exception {
    if (args.length < 1) {
        System.err.println("Usage: RedditRepostTiming <file>");
        System.exit(1);
    }

    String InputPath = args[0];
    
    // 1
    SparkSession spark = SparkSession.builder().appName("RedditRepostTiming").getOrCreate();

    // 2
    StructType schema = new StructType()
        .add("image_id", DataTypes.LongType, true)
        .add("unixtime", DataTypes.LongType, true)
        .add("post_title", DataTypes.StringType, true)
        .add("subreddit", DataTypes.StringType, true)
        .add("upvotes", DataTypes.IntegerType, true)
        .add("downvotes", DataTypes.IntegerType, true)
        .add("comments", DataTypes.IntegerType, true);

    // 3
    Dataset<Row> redditData = spark.read().schema(schema).csv(InputPath);

    // 4
    redditData = redditData.withColumn("timestamp", from_unixtime(col("unixtime"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
                            .withColumn("timestamp_est", expr("from_utc_timestamp(timestamp, 'America/New_York')"))
                            .drop("timestamp");

    // 5
    redditData = redditData.withColumn("hour", hour(col("timestamp_est")));

    // 6
    redditData = redditData.withColumn("impact", col("upvotes").plus(col("downvotes")).plus(col("comments")));

    // 7
    Dataset<Row> hourImpact = redditData.groupBy("hour").agg(sum("impact").alias("total_impact"));

    // 8
    hourImpact.select("hour", "total_impact").orderBy("hour").show(24, false);

    spark.stop();
	}

}
