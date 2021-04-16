package uk.co.newday.solution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, rank, collect_list}
import org.apache.spark.sql.expressions.Window

object Exercise3UserTop3Movies {

  def execute(movies: DataFrame, rating: DataFrame)= {

    val ratingWindow = Window.partitionBy(col("userId")).orderBy(col("rating").desc)

    val rankedRating = rating.withColumn("movieRank", rank().over(ratingWindow))

    val topFilteredRating = rankedRating.filter(col("movieRank").leq(3))

    val topMovies = topFilteredRating.join(movies, Seq("movieId"), "left")

    val topMoviesAgg = topMovies.groupBy("userId").agg(collect_list(col("title")).as("favMovieList"))

    topMoviesAgg

  }

}
