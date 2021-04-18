package uk.co.newday

import java.io.FileNotFoundException

import scala.io.Source

object TestUtilities {

  val loadAppProperties = () => {

    if(Constants.prop.size() == 0){

      val url = getClass.getResource("/application.properties")

      if (url != null) {
        val source = Source.fromURL(url)
        Constants.prop.load(source.bufferedReader())
      }
      else{
        throw new FileNotFoundException("Properties file cannot be loaded")
      }
    }
  }

}
