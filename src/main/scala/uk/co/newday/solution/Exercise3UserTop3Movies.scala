package uk.co.newday.solution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, rank}
import org.apache.spark.sql.expressions.Window
import uk.co.newday.Constants

object Exercise3UserTop3Movies {

  def execute(movies: DataFrame, rating: DataFrame)= {

    val ratingWindow = Window.partitionBy(col("userId")).orderBy(col("rating").desc)

    val rankedRating = rating.withColumn("movieRank", rank().over(ratingWindow))

    val topFilteredRating = rankedRating.filter(col("movieRank").leq(Constants.prop.getProperty("top_ratings").toInt))

    val topMovies = topFilteredRating.join(movies, Seq("movieId"), "left")

    val topMoviesAgg = topMovies.groupBy("userId").agg(collect_list(col("title")).as("favMovieList"))

    topMoviesAgg

  }

}
