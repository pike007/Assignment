package uk.co.newday

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.FunSpec
import uk.co.newday.solution.{Exercise1ReadFiles, Exercise2MovieRatings}

class Exercise2MovieRatingsTest extends FunSpec with TestSparkSession {

  TestUtilities.loadAppProperties()

  val (movies, ratings) = Exercise1ReadFiles.execute(spark)
  val movieRatings = Exercise2MovieRatings.execute(movies, ratings).cache()

  describe("Test Exercise2MovieRatings.execute..."){

    it("Check max rating for MOVIE1"){
      assertResult(5){
        movieRatings.filter(col("title") === "MOVIE1").select("maxMovieRating").collect()(0).getInt(0)
      }
    }
    it("Check min rating for MOVIE1"){
      assertResult(3){
        movieRatings.filter(col("title") === "MOVIE1").select("minMovieRating").collect()(0).getInt(0)
      }
    }
    it("Check avg rating for MOVIE1"){
      assertResult(BigDecimal(4.0)){
        BigDecimal(movieRatings.filter(col("title") === "MOVIE1").select("avgMovieRating").collect()(0).getDouble(0))
      }
    }

    it("Check max rating for MOVIE5(no rating given)"){
      assertResult(null){
        movieRatings.filter(col("title") === "MOVIE5").select("maxMovieRating").collect()(0).get(0)
      }
    }

    it("Check min rating for MOVIE5(no rating given)"){
      assertResult(null){
        movieRatings.filter(col("title") === "MOVIE5").select("minMovieRating").collect()(0).get(0)
      }
    }
    it("Check avg rating for MOVIE5(no rating given)"){
      assertResult(null){
        movieRatings.filter(col("title") === "MOVIE5").select("avgMovieRating").collect()(0).get(0)
      }
    }
  }

}
