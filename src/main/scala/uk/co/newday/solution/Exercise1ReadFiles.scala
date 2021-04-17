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
      val numCols = cols.length

      Movie(if(numCols > 0) cols(0).toInt else 0, if(numCols > 1) cols(1) else "DEFAULT",
        if(numCols > 2) cols(2) else "DEFAULT")
    }

    val parseRatingRow = (row: Row) => {

      val cols = row.getString(0).split(Constants.prop.getProperty("delimiter"))
      val numCols = cols.length

      Rating(if(numCols > 0) cols(0).toInt else 0, if(numCols > 1) cols(1).toInt else 0,
        if(numCols > 2) cols(2).toInt else 0, if(numCols > 3) cols(3).toInt else 0)
    }

    val loadCSV = (inputPath: String) => {
      session.read.option("delimiter", "\u0001").format("csv").load(inputPath)
    }

    val movies = loadCSV(Constants.prop.getProperty("input_movie_file_path"))
      .map(x => parseMovieRow(x)).toDF().cache()

    val ratings = loadCSV(Constants.prop.getProperty("input_ratings_file_path"))
      .map(x => parseRatingRow(x)).toDF().cache()

    (movies, ratings)

  }


}
