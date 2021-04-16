package uk.co.newday.solution

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Exercise1ReadFiles {

  private case class Movie (movieId:Int, title:String, genre:String)
  private case class Rating (userId:Int, movieId:Int, rating:Int, timestamp:Int)

  def execute(session: SparkSession): (DataFrame, DataFrame) = {

    import session.implicits._

    val parseMovieRow = (row: Row) => {
      val cols = row.getString(0).split("::")
      Movie(cols(0).toInt, cols(1), cols(2))
    }

    val parseRatingRow = (row: Row) => {
      val cols = row.getString(0).split("::")
      Rating(cols(0).toInt, cols(1).toInt, cols(2).toInt, cols(2).toInt)
    }

    val movies = session.read.option("delimiter", "\u0001").format("csv").load("C:\\pike\\assignments\\NewDay\\ml-1m\\movies.dat")
      .map(x => parseMovieRow(x)).toDF().cache()

    val ratings = session.read.option("delimiter", "\u0001").format("csv").load("C:\\pike\\assignments\\NewDay\\ml-1m\\ratings.dat")
      .map(x => parseRatingRow(x)).toDF().cache()

    (movies, ratings)

  }


}
