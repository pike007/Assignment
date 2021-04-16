package uk.co.newday.solution

import org.apache.spark.sql.DataFrame

object Exercise4WriteParquet {

  def execute(movies: DataFrame, ratings: DataFrame, movieRatings:DataFrame, ratingWithRankTop3:DataFrame) = {

    movies.coalesce(1).write.mode("overwrite").parquet("C:\\pike\\assignments\\NewDay\\movies")
    ratings.coalesce(1).write.mode("overwrite").parquet("C:\\pike\\assignments\\NewDay\\ratings")
    movieRatings.coalesce(1).write.mode("overwrite").parquet("C:\\pike\\assignments\\NewDay\\movieRatings")
    ratingWithRankTop3.coalesce(1).write.mode("overwrite").parquet("C:\\pike\\assignments\\NewDay\\top3movies")
  }

}
