package com.RUSpark;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/* any necessary Java packages here */

/**
1- Read ratings data into RDD
2- Calculate similarities with other users
3- Recommend movies for the user
4- Print recommendations
 */
public class NetflixCollabFilter {

  private static final int K = 11;


	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixCollabFilter <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
    
		/* Implement Here */ 
    int userId = Integer.parseInt(args[1]);

    JavaSparkContext sc = new JavaSparkContext("local", "CollaborativeFiltering");

    // Load ratings data (userId, movieId, rating)
    JavaRDD<String> ratingsRawData = sc.textFile(InputPath);
    JavaPairRDD<Integer, Tuple2<Integer, Double>> ratingsData = parseRatingsData(ratingsRawData);

    // Specify target userId
    int targetUserId = userId;

    // Calculate user similarities
    
    JavaPairRDD<Integer, Double> userSimilarities = calculateUserSimilarities(ratingsData, targetUserId);

    // Make recommendations
    JavaPairRDD<Double, Double> recommendations = makeRecommendations(ratingsData, userSimilarities, targetUserId);

    // Output recommendations
    recommendations.collect().forEach(System.out::println);

    // Stop Spark context
    sc.stop();
    }

  private static JavaPairRDD<Integer, Tuple2<Integer, Double>> parseRatingsData(JavaRDD<String> rawData) {
      // Your code to parse ratings data into (userId, (movieId, rating)) format
      // Example:
      return rawData.mapToPair(line -> {
          // Your code to parse a line of ratings data
          // Example:
          String[] parts = line.split(",");
          int userId = Integer.parseInt(parts[0]);
          int movieId = Integer.parseInt(parts[1]);
          double rating = Double.parseDouble(parts[2]);

          return new Tuple2<>(userId, new Tuple2<>(movieId, rating));
      });
  }

  private static JavaPairRDD<Integer, Double> calculateUserSimilarities(
        JavaPairRDD<Integer, Tuple2<Integer, Double>> ratingsData,
        int targetUserId) {
    // Your code to calculate user similarities
    // Example:
    List<Double> targetUserRatings = getRatings(targetUserId, ratingsData).stream().map(Tuple2::_2).collect(Collectors.toList());

    return ratingsData
            .filter(tuple -> tuple._1() != targetUserId)
            .mapToPair(tuple -> {
                int otherUserId = tuple._1();
                List<Double> otherUserRatings = getRatings(otherUserId, ratingsData).stream().map(Tuple2::_2).collect(Collectors.toList());

                double similarity = calculateSimilarity(targetUserRatings, otherUserRatings);

                return new Tuple2<>(otherUserId, similarity);
            })
            .sortByKey(false);
}

  

    private static List<Tuple2<Double, Double>> getRatings(int userId, JavaPairRDD<Integer, Tuple2<Integer, Double>> ratingsData) {
      // Your code to get ratings for a user
      // Example:
      List<Tuple2<Double, Double>> ratingsList = ratingsData
              .filter(tuple -> tuple._1() == userId)
              .map(tuple -> new Tuple2<>(Double.valueOf(tuple._2()._1()), tuple._2()._2()))
              .take(Integer.MAX_VALUE); // Use take() instead of collect()

      return ratingsList.stream().collect(Collectors.toList());
    }

    private static double calculateSimilarity(List<Double> x, List<Double> y) {
      // Calculate the mean of the vectors x and y.
      double xMean = x.stream().mapToDouble(Double::doubleValue).sum() / x.size();
      double yMean = y.stream().mapToDouble(Double::doubleValue).sum() / y.size();

      // Center the vectors x and y by subtracting the mean rating for each user.
      List<Double> centeredX = x.stream().map(value -> value - xMean).collect(Collectors.toList());
      List<Double> centeredY = y.stream().map(value -> value - yMean).collect(Collectors.toList());

      // Calculate the dot product of the centered vectors x and y.
      double dotProduct = 0.0;
      for (int i = 0; i < centeredX.size(); i++) {
          dotProduct += centeredX.get(i) * centeredY.get(i);
      }

      // Calculate the cosine similarity of the centered vectors x and y.
      double cosineSimilarity = dotProduct / (Math.sqrt(x.size()) * Math.sqrt(y.size()));

      return cosineSimilarity;
    }
    private static JavaPairRDD<Double, Double> makeRecommendations(
        JavaPairRDD<Integer, Tuple2<Integer, Double>> ratingsData,
        JavaPairRDD<Integer, Double> userSimilarities,
        int targetUserId) {

    // Filter out the target user from the similarities data.
    JavaPairRDD<Integer, Double> filteredSimilarities = userSimilarities.filter(tuple -> tuple._1() != targetUserId);

    // Calculate the weighted average rating for each movie based on the similarities to the target user.
    JavaPairRDD<Double, Double> recommendations = filteredSimilarities.flatMapToPair(tuple -> {
        int otherUserId = tuple._1();
        List<Tuple2<Double, Double>> otherUserRatings = getRatings(otherUserId, ratingsData);
        double similarity = tuple._2();

        double weightedAverageRating = 0.0;
        double id = 0.0;
        for (Tuple2<Double, Double> rating : otherUserRatings) {
            Double movieId = rating._1();
            id = movieId;
            double otherUserRating = rating._2();

            // Only recommend movies that the target user has not rated yet.
            if (!hasRatedMovie(movieId, ratingsData, targetUserId)) {
                weightedAverageRating += similarity * otherUserRating;
            }
        }

        return Collections.singletonList(new Tuple2<>(id, weightedAverageRating)).iterator();
    });

    // Reduce the recommendations by movie ID to get the final recommendations.
    return recommendations.reduceByKey(Double::sum);
}
  private static boolean hasRatedMovie(Double movieId2, JavaPairRDD<Integer, Tuple2<Integer, Double>> ratingsData, int movieId) {
      // Your code to check if a user has rated a movie
      // Example:
      return ratingsData
              .filter(tuple -> (double) tuple._1() == movieId2 && tuple._2()._1() == movieId)
              .count() > 0;
  }

}
