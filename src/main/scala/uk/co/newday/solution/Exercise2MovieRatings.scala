package uk.co.newday.solution

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, max, min, avg}

object Exercise2MovieRatings {

  def execute(movies: DataFrame, ratings: DataFrame)= {

    val movieRatingJoin = movies.join(ratings,
      Seq("movieId"),
      "left")

    val movieRatings = movieRatingJoin.groupBy("movieId", "title", "genre").agg(
      max(col("rating")).as("maxMovieRating"),
      min(col("rating")).as("minMovieRating"),
      avg(col("rating")).as("avgMovieRating")
    )

    movieRatings
  }

}
