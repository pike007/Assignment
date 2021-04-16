package uk.co.newday

import org.apache.spark.sql.SparkSession
import uk.co.newday.solution.{Exercise1ReadFiles, Exercise2MovieRatings, Exercise3UserTop3Movies, Exercise4WriteParquet}

object MovieRatings {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("Movie Rating")
        .master("local")
      .getOrCreate()

    val (movies, ratings) = Exercise1ReadFiles.execute(session)
    val movieRatings = Exercise2MovieRatings.execute(movies, ratings)
    val ratingWithRankTop3 = Exercise3UserTop3Movies.execute(movies, ratings)
    Exercise4WriteParquet.execute(movies, ratings, movieRatings, ratingWithRankTop3)


  }

}
