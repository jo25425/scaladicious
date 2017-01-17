import java.io.File

import breeze.linalg.{*, Axis, DenseMatrix, DenseVector}
import breeze.stats._
import com.fasterxml.jackson.module.scala.OptionModule

import math.Pi
import nak.cluster.GDBSCAN.Cluster
import nak.cluster._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import sys.exit


/**
  * Created by joanne on 27/12/2016.
  */
object Worker {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val usage = """
    Usage: spark-dbscan [--method mean|median] [--reuse none|locations|predictions|all] filename
  """
  type OptionMap = Map[Symbol, Any]

  def processArgs(args: Array[String]): OptionMap = {
    if (args.length == 0) println(usage)
    val argList = args.toList

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = s(0) == '-'

      list match {
        case Nil => map
        case "--method" :: string :: tail
          if string matches "mean|median" =>
          nextOption(map ++ Map('method -> string), tail)
        case "--distance" :: string :: tail
          if string matches "haversine|euclidean" =>
          nextOption(map ++ Map('distance -> string), tail)
        case "--dir" :: string :: tail =>
          nextOption(map ++ Map('dir -> string), tail)
        case "--reuse" :: string :: tail
          if string matches "none|locations|predictions|all" =>
          nextOption(map ++ Map('reuse -> string), tail)
        case string :: opt2 :: _ if isSwitch(opt2) =>
          nextOption(map ++ Map('inFile -> string), list.tail)
        case string :: Nil =>
          nextOption(map ++ Map('inFile -> string), list.tail)
        case option :: _ =>
          println("Unknown option "+option)
          exit(1)
      }
    }

    val options = nextOption(Map(), argList)
    if (options('reuse).toString == "none" && !options.isDefinedAt('inFile)) {
      println("Input file argument required when not reusing locations")
      exit(1)
    }
    options
  }


  def main(args: Array[String]) {

    val options = processArgs(args)
    val dataFile = options.getOrElse('inFile, "").toString
    val reuse = options.getOrElse('reuse, "locations").toString // none|locations|predictions|all
    val centreMethod = options.getOrElse('method, "mean").toString // mean|median
    val distanceString = options.getOrElse('distance, "haversine").toString // haversine|euclidean
    val outputDir = options.getOrElse('dir, "./data").toString

    val tweetLocationsPath = new File(outputDir, "tweet_locations").toString()
    val authorLocationsPath = new File(outputDir, "author_locations").toString()
    val predictionsPath = new File(outputDir, "predictions").toString()
    val distance = if (distanceString == "haversine") Kmeans.haversine else Kmeans.euclideanDistance

    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .getOrCreate()


    // Extract tweet locations and group them by author
    val tweetLocationsRdd: RDD[(Long, DenseMatrix[Double])] =
      if ((reuse matches "locations|predictions|all")
        && new java.io.File(tweetLocationsPath).exists)
        loadTweetLocations(sparkSession, tweetLocationsPath)
      else
        extractTweetLocations(sparkSession, dataFile, tweetLocationsPath)


    // Extract author locations
    val authorLocationsRdd: RDD[(Long, (Double, Double))] =
      if ((reuse matches "locations|predictions|all")
        && new java.io.File(authorLocationsPath).exists)
        loadAuthorLocations(sparkSession, authorLocationsPath)
      else
        extractAuthorLocations(sparkSession, dataFile, authorLocationsPath)


    // Predict author locations from tweet locations
    val epsilon = 1.0E-5
    val minPoints = 1
    val config = (minPoints, epsilon, centreMethod, distance)

    val predictionsRdd: RDD[(Long, Seq[(Double, Double)])] =
      if ((reuse matches "predictions|all")
        && new java.io.File(predictionsPath).exists)
        loadPredictions(sparkSession, predictionsPath)
      else
        predict(tweetLocationsRdd, predictionsPath, config)


    // Evaluation
    val score = evaluate(authorLocationsRdd, predictionsRdd, distance)
    println("Average distance to actual author location: %.5f km"
      .format(if (distance == Kmeans.euclideanDistance) toKilometers(score) else score))
    val countPredicted: Double = predictionsRdd.count()
    val countTotal: Double = authorLocationsRdd.count()
    val pct = countPredicted / countTotal * 100f
    println(s"Predicted $countPredicted of total $countTotal")
    println(s"Percentage predicted: $pct")

//    val (bestConfig, bestScore) = optimise(tweetLocationsRdd, authorLocationsRdd, method)
//    println(s"\nBest config: ${bestConfig}")
//    println(s"Best score: ${bestScore}")

  }

  def optimise(tweetLocationsRdd: RDD[(Long, DenseMatrix[Double])],
               authorLocationsRdd: RDD[(Long, (Double, Double))],
               optimiseConfig: (
                 String,
                 (DenseVector[Double], DenseVector[Double]) => Double)):
  ((Int, Double), (Double, Double)) = {

    val (centreMethod, distance) = optimiseConfig

    // The hyperparameter space to try out
    val epsilonChoices = List(1.0E-6, 1.0E-5, 1.0E-4)
    val minPointsChoices = List(1, 2, 3, 5, 10, 12)

    // Stores the most optimal configuration
    var bestParams = (0, 0.0)
    var bestScores = (1000.0, 0.0)

    def combineScores(scores: (Double, Double)): Double =
      (if (scores._1 != 0) 1 / 1 + scores._1 else 0) + scores._2 / 100

    // Iterate through possible parameter space
    for {
      e <- epsilonChoices
      m <- minPointsChoices
    } {
      val params = (m, e)
      val config = (m, e, centreMethod, distance)
      val predictionsRdd: RDD[(Long, Seq[(Double, Double)])] =
        predict(tweetLocationsRdd, null, config)

      // Evaluation
      val avgError = evaluate(authorLocationsRdd, predictionsRdd, distance)
      val countPredicted: Double = predictionsRdd.count()
      val countTotal: Double = authorLocationsRdd.count()
      val pctPredicted = countPredicted / countTotal * 100f
      println(f"[mL = $m, e = $e] Resulting score: $avgError%.2f average error, $pctPredicted%.2f predicted")
      println(combineScores(avgError, pctPredicted), combineScores(bestScores))

      if (pctPredicted > 0 &&
        combineScores(avgError, pctPredicted) > combineScores(bestScores)) {
        bestParams = params
        bestScores = (avgError, pctPredicted)
      }
    }

    (bestParams, bestScores)
  }

  def loadTweetLocations(sparkSession: SparkSession, inputPath: String):
  RDD[(Long, DenseMatrix[Double])] = {
    println(s"Loading locations from $inputPath...")
    sparkSession.sparkContext.objectFile(inputPath)
  }

  def loadAuthorLocations(sparkSession: SparkSession, inputPath: String):
  RDD[(Long, (Double, Double))] = {
    println(s"Loading locations from $inputPath...")
    sparkSession.sparkContext.objectFile(inputPath)
  }

  def loadPredictions(sparkSession: SparkSession, inputPath: String):
  RDD[(Long, Seq[(Double, Double)])] = {
    println(s"Loading locations from $inputPath...")
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
              config: (
                Int,
                Double,
                String,
                (DenseVector[Double], DenseVector[Double]) => Double)):
  RDD[(Long, Seq[(Double, Double)])] = {

    println(s"Making author location predictions...")

    val minLocations = config._1
    val predictions = tweetLocationsRdd
//      .filter(_._2.rows >= minLocations)
      .mapValues(locationsMatrix =>
        if (locationsMatrix.rows >= minLocations)
          runDbscan(locationsMatrix, config)
        else
          locationsMatrix(*, ::).map(v => (v(0), v(1))).data.toList
      )

    // Save predictions by user
    if (outputPath.nonEmpty) {
      FileUtils.deleteQuietly(new File(outputPath)) // Delete if already exists
      predictions.saveAsObjectFile(outputPath)
    }

    predictions
  }

  def toKilometers(a: Double): Double = {
    val R = 6372.8  //earth radius in km
    a / 180 * Pi * R // great-circle distance
  }

  def runDbscan(datapoints : breeze.linalg.DenseMatrix[Double],
                config: (
                  Int,
                  Double,
                  String,
                  (DenseVector[Double], DenseVector[Double]) => Double)):
  Seq[(Double, Double)] = {

    // DBSCAN parameters
    val (minPoints, epsilon, centreMethod, distance) = config

    val gdbscan = new GDBSCAN(
      DBSCAN.getNeighbours(epsilon = epsilon, distance = distance),
      DBSCAN.isCorePoint(minPoints = minPoints)
    )
    val clusters = gdbscan.cluster(datapoints)

    // Compute centres from clusters
    clusters.map(computeCentre(_, centreMethod))
  }

  def computeCentre(cluster: Cluster[Double], method: String): (Double, Double) = {
    val vectors = new DenseMatrix(2, cluster.points.size, cluster.points.flatMap(_.value.toArray).toArray).t

    if (method == "median") {
      val medianVector = median(vectors, Axis._0)
      (medianVector(0), medianVector(1))
    } else {
      val meanAndVarianceVector = meanAndVariance(vectors, Axis._0)
      (meanAndVarianceVector(0).mean, meanAndVarianceVector(1).mean)
    }
  }

  def evaluate(authorLocations: RDD[(Long, (Double, Double))],
               predictions: RDD[(Long, Seq[(Double, Double)])],
               distance: (DenseVector[Double], DenseVector[Double]) => Double):
  Double =

    (authorLocations join predictions)
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


}
