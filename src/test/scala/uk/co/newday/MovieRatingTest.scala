package uk.co.newday

import java.io.FileNotFoundException

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.Row
import org.scalatest.FunSpec
import uk.co.newday.solution.{Exercise1ReadFiles, Exercise2MovieRatings, Exercise3UserTop3Movies, Exercise4WriteParquet}

import scala.io.Source

class MovieRatingTest extends FunSpec {

  val spark = SparkSession
    .builder()
    .appName("Movie Rating Tests")
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

  val (movies, ratings) = Exercise1ReadFiles.execute(spark)
  val movieRatings = Exercise2MovieRatings.execute(movies, ratings).cache()
  val ratingWithRankTop3 = Exercise3UserTop3Movies.execute(movies, ratings).cache()
  
  describe("Test Exercise1ReadFiles.execute..."){

    it("Check if movies has expected columns"){
      assertResult(Array("movieId", "title", "genre")){
        movies.columns
      }
    }

    it("Check if movies info populated for movieId=1"){
      assertResult(Row(1,"MOVIE1","Animation|Children's|Comedy")){
        movies.filter(col("movieId") === 1).collect()(0)
      }
    }
    it("Check If faulty record is parsed"){
      assertResult(Row(6,"MOVIE6","DEFAULT")){
        movies.filter(col("movieId") === 6).collect()(0)
      }
    }

    it("Check if ratings has expected columns"){
      assertResult(Array("userId", "movieId", "rating", "timestamp")){
        ratings.columns
      }
    }
    it("Check if ratings info populated for userId=1 and movieId=1"){
      assertResult(Row(1, 1, 5, 978300760)){
        ratings.filter(col("userId") === 1 && col("movieId") === 1).collect()(0)
      }
    }

    it("Check if ratings info populated for faulty record userId=6 and movieId=6"){
      assertResult(Row(6, 6, 0, 0)){
        ratings.filter(col("userId") === 6 && col("movieId") === 6).collect()(0)
      }
    }

  }

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
