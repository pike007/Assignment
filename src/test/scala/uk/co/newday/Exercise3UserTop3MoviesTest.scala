package uk.co.newday

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.FunSpec
import uk.co.newday.solution.{Exercise1ReadFiles, Exercise2MovieRatings, Exercise3UserTop3Movies}

class Exercise3UserTop3MoviesTest extends FunSpec with TestSparkSession {

  TestUtilities.loadAppProperties()

  val (movies, ratings) = Exercise1ReadFiles.execute(spark)
  val movieRatings = Exercise2MovieRatings.execute(movies, ratings)
  val ratingWithRankTop3 = Exercise3UserTop3Movies.execute(movies, ratings).cache()

  describe("Test Exercise3UserTop3Movies.execute..."){

    it("top movies of userId =1"){
      assertResult(Array("MOVIE1", "MOVIE2", "MOVIE3")){
        ratingWithRankTop3.filter(col("userId") === 1).select("favMovieList").collect()(0).getList(0).toArray
      }
    }
    it("top movies of userId =2"){
      assertResult(Array("MOVIE4", "MOVIE2", "MOVIE1")){
        ratingWithRankTop3.filter(col("userId") === 2).select("favMovieList").collect()(0).getList(0).toArray
      }
    }

  }

}
