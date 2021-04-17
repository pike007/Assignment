package uk.co.newday.solution

import org.apache.spark.sql.DataFrame
import uk.co.newday.Constants

object Exercise4WriteParquet {

  def execute(movies: DataFrame, ratings: DataFrame, movieRatings:DataFrame, ratingWithRankTop3:DataFrame) = {

    writeIntoParquet(movies, Constants.prop.getProperty("output_movie_file_path"))
    writeIntoParquet(ratings, Constants.prop.getProperty("output_ratings_file_path"))
    writeIntoParquet(movieRatings, Constants.prop.getProperty("output_movieRatings_file_path"))
    writeIntoParquet(ratingWithRankTop3, Constants.prop.getProperty("output_ratingWithRankTop3_file_path"))
  }

  val writeIntoParquet = (finalDF: DataFrame, outputPath: String) => {

    finalDF.coalesce(1).write.mode("overwrite").parquet(outputPath)
  }

}
