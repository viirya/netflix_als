import java.util.Random
import java.io.{FileInputStream, DataInputStream, BufferedReader, InputStreamReader}

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.io.Source
import scala.collection.mutable.Seq

import com.github.nscala_time.time.Imports._

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

    val conf = new SparkConf().setAppName("NetflixALS")
    val sc = new SparkContext(conf)

    // load ratings and movie titles

    val datasetHomeDir = args(0)
    val movieTitleFile = args(1)

    val movies = readAndParseMovieTitles(movieTitleFile)
    val ratings = loadNetflixRatings(datasetHomeDir, movies, sc)
    val (training, validation) = getTrainingRatings(ratings)
    train(training, validation)

    sc.stop();
  }

  def train(training: RDD[Rating], validation: RDD[Rating]) = {
    val numTraining = training.count
    val numValidation = validation.count

    println("Training: " + numTraining + ", validation: " + numValidation)

    // train models and evaluate them on the validation set

    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
  }

  def getTrainingRatings(ratings: RDD[(Long, Rating)]):
    (RDD[Rating], RDD[Rating]) = {

    val training = ratings.filter(x => x._1 < 8)
                          .values
                          .persist
    val validation = ratings.filter(x => x._1 >=8)
                            .values
                            .persist
    (training, validation)
  }

  def loadNetflixRatings(dir: String, moviesMap: Map[Int, String], sc: SparkContext) = {
    var ratingRDD: RDD[(Long, Rating)]  = null
    moviesMap.foreach { (kv) =>
      val movieId = kv._1

      val ratings = sc.textFile(f"$dir/mv_$movieId%07d.txt").flatMap[(Long, Rating)] { line =>
        val fields = line.split(",")
        if (fields.size == 3) { 
          // format: (date, Rating(userId, movieId, rating))
          val timestamp = DateTime.parse(fields(2)).getMillis() / 1000
          Seq((timestamp % 10, Rating(fields(0).toInt, movieId, fields(1).toDouble)))
        } else {
          Seq()
        }
      }
      if (ratingRDD == null) {
        ratingRDD = ratings
      } else {
        ratingRDD = ratingRDD.union(ratings)
      }        
    }
    ratingRDD.persist
    ratingRDD
  }

  def readFile(path: String): Seq[String] = {
    var lines = Seq[String]()
    try {
      val fstream = new FileInputStream(path)
      val in = new DataInputStream(fstream)
      val br = new BufferedReader(new InputStreamReader(in))
      var line: String = null
      line = br.readLine()
      while (line != null)   {
        lines = lines.+:(line)
        line = br.readLine()
      }
      in.close()
    } catch {
      case e: Exception =>
        println("Error: " + e.getMessage())
    }
    lines
  } 

  def readAndParseMovieTitles(path: String): Map[Int, String] = {
    readFile(path).map { (line) =>
      val fields = line.split(",")
      (fields(0).toInt, fields(2))
    }.toMap
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
