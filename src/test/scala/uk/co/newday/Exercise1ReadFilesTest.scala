package uk.co.newday

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.{FunSpec, FunSuite}
import uk.co.newday.solution.Exercise1ReadFiles

class Exercise1ReadFilesTest extends FunSpec with  TestSparkSession {

  TestUtilities.loadAppProperties()

  val (movies, ratings) = Exercise1ReadFiles.execute(spark)

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
}
