package uk.co.newday.solution

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import uk.co.newday.Constants

object Exercise1ReadFiles {

  private case class Movie (movieId:Int, title:String, genre:String)
  private case class Rating (userId:Int, movieId:Int, rating:Int, timestamp:Int)

  def execute(session: SparkSession): (DataFrame, DataFrame) = {

    import session.implicits._

    val parseMovieRow = (row: Row) => {
      val cols = row.getString(0).split(Constants.prop.getProperty("delimiter"))
      Movie(cols(0).toInt, cols(1), cols(2))
    }

    val parseRatingRow = (row: Row) => {
      val cols = row.getString(0).split(Constants.prop.getProperty("delimiter"))
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(2).toInt)
    }

    val movies = session.read.option("delimiter", "\u0001").format("csv")
      .load(Constants.prop.getProperty("input_movie_file_path"))
      .map(x => parseMovieRow(x)).toDF().cache()

    val ratings = session.read.option("delimiter", "\u0001").format("csv")
      .load(Constants.prop.getProperty("input_ratings_file_path"))
      .map(x => parseRatingRow(x)).toDF().cache()

    (movies, ratings)

  }


}
