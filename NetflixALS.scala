import java.util.Random
import java.io.{FileInputStream, DataInputStream, BufferedReader, InputStreamReader}

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.io.Source
import scala.collection.mutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

object NetflixALS {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)	

    if (args.length != 2) {
      println("Usage: NetflixALS \"datasetHomeDir\" \"path to movie_title.txt\"")
      exit(1)
    }

    // set up environment

    //val conf = new SparkConf().setAppName("NetflixALS")
    //val sc = new SparkContext(conf)

    // load ratings and movie titles

    val datasetHomeDir = args(0)
    val movieTitleFile = args(1)

    val movies = readAndParseMovieTitles(movieTitleFile)

    //sc.stop();
  }

  def readAndParseMovieTitles(path: String): Map[Int, String] = {
    val movieMap = Map[Int, String]() 
 
    try {
      val fstream = new FileInputStream(path)
      val in = new DataInputStream(fstream)
      val br = new BufferedReader(new InputStreamReader(in))
      var line: String = null
      line = br.readLine()
      while (line != null)   {
        val fields = line.split(",")
        movieMap += ((fields(0).toInt, fields(2)))
        line = br.readLine()
      }
      in.close()
    } catch {
      case e: Exception =>
        println("Error: " + e.getMessage())
    }
    movieMap
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
                                           .join(data.map(x => ((x.user, x.product), x.rating)))
                                           .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }
  
  /** Elicitate ratings from command-line. */
  def elicitateRatings(movies: Seq[(Int, String)]) = {
    val prompt = "Please rate the following movie (1-5 (best), or 0 if not seen):"
    println(prompt)
    val ratings = movies.flatMap { x =>
      var rating: Option[Rating] = None
      var valid = false
      while (!valid) {
        print(x._2 + ": ")
        try {
          val r = Console.readInt
          if (r < 0 || r > 5) {
            println(prompt)
          } else {
            valid = true
            if (r > 0) {
              rating = Some(Rating(0, x._1, r))
            }
          }
        } catch {
          case e: Exception => println(prompt)
        }
      }
      rating match {
        case Some(r) => Iterator(r)
        case None => Iterator.empty
      }
    }
    if(ratings.isEmpty) {
      error("No rating provided!")
    } else {
      ratings
    }
  }
}
