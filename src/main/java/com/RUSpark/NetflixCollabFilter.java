package com.RUSpark;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/* any necessary Java packages here */

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
    // M value
    int numRecommendations = Integer.parseInt(args[2]);

    JavaSparkContext sc = new JavaSparkContext("local", "CollaborativeFiltering");

    // Read ratings data into RDD
    JavaRDD<String> ratingsData = sc.textFile(InputPath);
    JavaPairRDD<Integer, List<Double>> userRatings = ratingsData.mapToPair(line -> {
      String[] parts = line.split(",");
      int userId2 = Integer.parseInt(parts[0]);
      int movieId = Integer.parseInt(parts[1]);
      double rating = Double.parseDouble(parts[2]);
  
      return new Tuple2<>(userId2, new Tuple2<>(movieId, rating));
  })
  .groupByKey()
  .mapValues(iterable -> {
      List<Double> ratingsList = new ArrayList<>(Collections.nCopies(1000, 0.0));
      for (Tuple2<Integer, Double> tuple : iterable) {
          ratingsList.set(tuple._1(), tuple._2());
      }
      return ratingsList;
  });
  
    // Calculate similarities with other users
    List<Tuple2<Integer, Double>> userSimilarities = userRatings
            .filter(tuple -> tuple._1() != userId)
            .mapToPair(tuple -> new Tuple2<>(tuple._1(), calculateSimilarity(userRatings.lookup(userId).get(0), tuple._2())))
            .sortByKey(false)
            .take(10); // Choose top 10 most similar users

    // Recommend movies for the user
    List<Tuple2<Integer, Double>> recommendations = recommendMovies(userId, userSimilarities, userRatings, numRecommendations);

    // Print recommendations
    System.out.println("Top " + numRecommendations + " movie recommendations for user " + userId + ":");
    for (Tuple2<Integer, Double> recommendation : recommendations) {
        System.out.println("Movie ID: " + recommendation._1() + ", Predicted Rating: " + recommendation._2());
    }

    sc.stop();
	}

  // Function to calculate centered cosine similarity between two users
  private static double calculateSimilarity(List<Double> u, List<Double> v) {
      // Calculate mean values
      double meanU = u.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
      double meanV = v.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

      double dotProduct = 0.0;
      double normU = 0.0;
      double normV = 0.0;

      // Calculate dot product and norms
      for (int i = 0; i < u.size(); i++) {
          double rUi = u.get(i) - meanU;
          double rVi = v.get(i) - meanV;

          dotProduct += rUi * rVi;
          normU += Math.pow(rUi, 2);
          normV += Math.pow(rVi, 2);
      }

      // Avoid division by zero
      if (normU == 0 || normV == 0) {
          return 0.0;
      }

      // Calculate centered cosine similarity
      return dotProduct / (Math.sqrt(normU) * Math.sqrt(normV));
  }

  // Function to recommend movies for a user based on similar users
  private static List<Tuple2<Integer, Double>> recommendMovies(int userId, List<Tuple2<Integer, Double>> userSimilarities,
                                                              JavaPairRDD<Integer, List<Double>> userRatings, int numRecommendations) {
      // Collect K most similar users
      List<Integer> similarUserIds = new ArrayList<>();
      for (Tuple2<Integer, Double> similarity : userSimilarities) {
          similarUserIds.add(similarity._1());
      }

      // Aggregate ratings of similar users
      JavaPairRDD<Integer, Double> recommendedRatings = userRatings
        .filter(tuple -> similarUserIds.contains(tuple._1()) && tuple._2().size() > 0)
        .flatMapToPair(tuple -> {
            int otherUserId = tuple._1();
            List<Double> otherUserRatings = tuple._2();
            List<Double> userRatingsList = userRatings.lookup(userId).get(0);

            List<Tuple2<Integer, Double>> recommendations = new ArrayList<>();
            for (int i = 0; i < userRatingsList.size(); i++) {
                if (userRatingsList.get(i) == 0.0 && otherUserRatings.get(i) > 0.0) {
                    recommendations.add(new Tuple2<>(i, otherUserRatings.get(i)));
                }
            }

            return recommendations.iterator();
        })
        .reduceByKey(Double::sum)
        .filter(tuple -> tuple._2() > 0.0)
        .mapToPair(tuple -> new Tuple2<>(tuple._1(), tuple._2() / K)); // Normalize by the number of similar users


      // Calculate weighted average of ratings
      JavaPairRDD<Integer, Double> aggregatedRatings = recommendedRatings
        .mapValues(value -> value / (double) similarUserIds.size());

      // Sort recommendations by rating in descending order
      List<Tuple2<Integer, Double>> sortedRecommendations = aggregatedRatings
              .sortByKey(false)
              .take(numRecommendations);

      return sortedRecommendations;
  }

}
