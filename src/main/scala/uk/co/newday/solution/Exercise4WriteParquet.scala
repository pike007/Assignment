package uk.co.newday.solution

import org.apache.spark.sql.DataFrame
import uk.co.newday.Constants

object Exercise4WriteParquet {

  def execute(movies: DataFrame, ratings: DataFrame, movieRatings:DataFrame, ratingWithRankTop3:DataFrame) = {

    movies.coalesce(1).write.mode("overwrite").parquet(Constants.prop.getProperty("output_movie_file_path"))
    ratings.coalesce(1).write.mode("overwrite").parquet(Constants.prop.getProperty("output_ratings_file_path"))
    movieRatings.coalesce(1).write.mode("overwrite").parquet(Constants.prop.getProperty("output_movieRatings_file_path"))
    ratingWithRankTop3.coalesce(1).write.mode("overwrite").parquet(Constants.prop.getProperty("output_ratingWithRankTop3_file_path"))
  }

}
