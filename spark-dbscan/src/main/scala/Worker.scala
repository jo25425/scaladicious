import java.io.File

import breeze.linalg.{Axis, DenseMatrix, DenseVector}
import breeze.numerics._
import breeze.stats._

import math.Pi
import nak.cluster.GDBSCAN.Cluster
import nak.cluster._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


/**
  * Created by joanne on 27/12/2016.
  */
object Worker {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  def main(args: Array[String]) {

    val method = "median"
    val dataFile = "./data/training_a.json"
    val tweetLocationsPath = "./data/tweet_locations"
    val authorLocationsPath = "./data/author_locations"
    val predictionsPath = "./data/predictions"
    val reuse = "Locations" // None, Locations, Predictions, All

    val sparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()


    // Extract tweet locations and group them by author
    val tweetLocationsRdd: RDD[(Long, DenseMatrix[Double])] =
      if ((reuse == "Locations" || reuse == "Predictions" || reuse == "All")
        && new java.io.File(tweetLocationsPath).exists)
        loadTweetLocations(sparkSession, tweetLocationsPath)
      else
        extractTweetLocations(sparkSession, dataFile, tweetLocationsPath)


    // Extract author locations
    val authorLocationsRdd: RDD[(Long, (Double, Double))] =
      if ((reuse == "Locations" || reuse == "Predictions" || reuse == "All")
        && new java.io.File(authorLocationsPath).exists)
        loadAuthorLocations(sparkSession, authorLocationsPath)
      else
        extractAuthorLocations(sparkSession, dataFile, authorLocationsPath)


    // Predict author locations from tweet locations
//    val epsilon = 0.00005
//    val minPoints = 4
//    val config = (minPoints, epsilon)

//    val predictionsRdd: RDD[(Long, Seq[(Double, Double)])] =
//      if ((reuse == "Predictions" || reuse == "All")
//        && new java.io.File(predictionsPath).exists)
//        loadPredictions(sparkSession, predictionsPath)
//      else
//        predict(tweetLocationsRdd, predictionsPath, method, config)
//
//
//    // Evaluation
//    val score = evaluate(authorLocationsRdd, predictionsRdd)
//    println("Average distance to actual author location: %.5f km".format(score))
//    val countPredicted: Double = predictionsRdd.count()
//    val countTotal: Double = authorLocationsRdd.count()
//    val pct = countPredicted / countTotal * 100f
//    println(s"Predicted $countPredicted of total $countTotal")
//    println(s"Percentage predicted: $pct")

    val (bestConfig, bestScore) = optimise(tweetLocationsRdd, authorLocationsRdd, method)
    println(s"\nBest config: ${bestConfig}")
    println(s"Best score: ${bestScore}")

  }

  def optimise(tweetLocationsRdd: RDD[(Long, DenseMatrix[Double])],
               authorLocationsRdd: RDD[(Long, (Double, Double))],
               method: String):
  ((Int, Double), (Double, Double)) = {

    // The hyperparameter space to try out
    val epsilonChoices = List(1.0E-5, 1.0E-4, 2*1.0E-4, 5*1.0E-4, 1.0E-3)
    val minPointsChoices = List(1, 2, 3, 5, 10, 12)

    // Stores the most optimal configuration
    var bestConfig = (0, 0.0)
    var bestScores = (1000.0, 0.0)

    def combineScores(scores: (Double, Double)): Double = scores._1 * (100-scores._2)

    // Iterate through possible parameter space
    for {
      e <- epsilonChoices
      m <- minPointsChoices
    } {
      val config = (m, e)
      val predictionsRdd: RDD[(Long, Seq[(Double, Double)])] =
        predict(tweetLocationsRdd, method, null, config)

      // Evaluation
      val avgError = evaluate(authorLocationsRdd, predictionsRdd)
      val countPredicted: Double = predictionsRdd.count()
      val countTotal: Double = authorLocationsRdd.count()
      val pctPredicted = countPredicted / countTotal * 100f
      println("Resulting score: %.2f average error, %.2f predicted".format(avgError, pctPredicted))
      println(combineScores(avgError, pctPredicted), combineScores(bestScores))

      if (combineScores(avgError, pctPredicted) < combineScores(bestScores)) {
        bestConfig = config
        bestScores = (avgError, pctPredicted)
      }
    }

    (bestConfig, bestScores)
  }

  def loadTweetLocations(sparkSession: SparkSession, inputPath: String):
  RDD[(Long, DenseMatrix[Double])] = {
    println(s"Loading locations from ${inputPath}...")
    sparkSession.sparkContext.objectFile(inputPath)
  }

  def loadAuthorLocations(sparkSession: SparkSession, inputPath: String):
  RDD[(Long, (Double, Double))] = {
    println(s"Loading locations from ${inputPath}...")
    sparkSession.sparkContext.objectFile(inputPath)
  }

  def loadPredictions(sparkSession: SparkSession, inputPath: String):
  RDD[(Long, Seq[(Double, Double)])] = {
    println(s"Loading locations from ${inputPath}...")
    sparkSession.sparkContext.objectFile(inputPath)
  }

  def extractTweetLocations(sparkSession: SparkSession, inputPath: String, outputPath: String):
  RDD[(Long, DenseMatrix[Double])] = {

    println(s"Extracting tweet locations from $inputPath...")

    val tweets = sparkSession.read.json(inputPath)

    // Group locations per author in a breeze DenseMatrix
    val locationsPerAuthor = tweets.rdd
      .map(loc => (
        loc.getAs[String]("user_id"),
        loc.getAs[String]("tweet_latitude"),
        loc.getAs[String]("tweet_longitude")
      ))
      .groupBy(_._1.toLong)
      .mapValues(tweets => {
        val (numRows, numCols) = (tweets.size, 2)
        val latitudes = tweets.map(_._2.toDouble)
        val longitudes = tweets.map(_._3.toDouble)
        val values = (latitudes ++ longitudes).toArray
        new DenseMatrix(numRows, numCols, values)
      })

    // Save locations by author
    FileUtils.deleteQuietly(new File(outputPath)) // Delete if already exists
    locationsPerAuthor.saveAsObjectFile(outputPath)

    locationsPerAuthor
  }

  def extractAuthorLocations(sparkSession: SparkSession, inputPath: String, outputPath: String):
  RDD[(Long, (Double, Double))] = {

    println(s"Extracting author locations from $inputPath...")

    val tweets = sparkSession.read.json(inputPath)

    // Get city location for each author
    val authorLocations = tweets.rdd
      .map(loc => (
        loc.getAs[String]("user_id"),
        loc.getAs[String]("user_city_latitude"),
        loc.getAs[String]("user_city_longitude")
      ))
      .groupBy(_._1.toLong)
      .mapValues(tweets => (tweets.head._2.toDouble, tweets.head._3.toDouble))

    // Save locations by user
    FileUtils.deleteQuietly(new File(outputPath)) // Delete if already exists
    authorLocations.saveAsObjectFile(outputPath)

    authorLocations
  }

  def predict(tweetLocationsRdd: RDD[(Long, DenseMatrix[Double])],
              outputPath: String,
              method: String,
              config: (Int, Double)):
  RDD[(Long, Seq[(Double, Double)])] = {

    println(s"Making author location predictions...")

    val minLocations = config._1
    val predictions = tweetLocationsRdd
      .filter(_._2.rows >= minLocations)
      .mapValues(runDbscan(_, method, config))

    // Save predictions by user
    if (outputPath.nonEmpty) {
      FileUtils.deleteQuietly(new File(outputPath)) // Delete if already exists
      predictions.saveAsObjectFile(outputPath)
    }

    predictions
  }

  def toDistance(a: Double): Double = {
    val R = 6372.8  //earth radius in km
    a / 180 * Pi * R // great-circle distance
  }

  def runDbscan(datapoints : breeze.linalg.DenseMatrix[Double],
                method: String,
                config: (Int, Double)):
  Seq[(Double, Double)] = {

    // DBSCAN parameters
    val (minPoints, epsilon) = config

    val gdbscan = new GDBSCAN(
      DBSCAN.getNeighbours(epsilon = epsilon, distance = Kmeans.euclideanDistance),
      DBSCAN.isCorePoint(minPoints = minPoints)
    )
    val clusters = gdbscan.cluster(datapoints)

    // Compute centres from clusters
    clusters.map(computeCentre(_, method))
  }

  def computeCentre(cluster: Cluster[Double], method: String): (Double, Double) = {
    val vectors = new DenseMatrix(2, cluster.points.size, cluster.points.flatMap(_.value.toArray).toArray).t

    val centre =
      if (method == "median") median(vectors, Axis._0).toArray
      else meanAndVariance(vectors, Axis._0).toArray.map(_.mean)

    (centre(0), centre(1))
  }

  def evaluate(authorLocations: RDD[(Long, (Double, Double))],
               predictions: RDD[(Long, Seq[(Double, Double)])]):
  Double = {

    val distance = Kmeans.euclideanDistance

    val avg = (authorLocations join predictions)
      .filter(_._2._2.nonEmpty) // only keep cases where predictions were made
      .map(entry => {
        val (actual, predicted) = entry._2

        val vectorActual = DenseVector(actual._1, actual._2)
        val (d, best) = predicted
          .map(l => (distance(DenseVector(l._1, l._2), vectorActual), l))
          .min

//      println(s"\n*** ${entry._1} *************")
//      println("Closest is (%.5f, %.5f) at %.5f ~ %.5f km".format(
//        best._1, best._2, d, toDistance(d)))
        d
      })
      .mean()

      toDistance(avg)
  }


}
