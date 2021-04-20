package uk.co.newday

import java.io.FileNotFoundException

import org.apache.spark.sql.SparkSession
import uk.co.newday.solution.{Exercise1ReadFiles, Exercise2MovieRatings, Exercise3UserTop3Movies, Exercise4WriteParquet}

import scala.io.Source

object MovieRatings {

  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("Movie Rating")
        .master("local")
      .getOrCreate()

    val url = getClass.getResource("/application.properties")

    if (url != null) {
      val source = Source.fromURL(url)
      Constants.prop.load(source.bufferedReader())
    }
    else{
      throw new FileNotFoundException("Properties file cannot be loaded")
    }

    val (movies, ratings) = Exercise1ReadFiles.execute(session)
    val movieRatings = Exercise2MovieRatings.execute(movies, ratings)
    val ratingWithRankTop3 = Exercise3UserTop3Movies.execute(movies, ratings)
    Exercise4WriteParquet.execute(movies, ratings, movieRatings, ratingWithRankTop3)

  }

}
