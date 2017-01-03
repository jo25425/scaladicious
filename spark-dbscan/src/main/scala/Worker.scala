import java.io.File

import breeze.linalg.{DenseMatrix, DenseVector, sum, Axis}
import breeze.stats._
import nak.cluster.GDBSCAN.Cluster
import nak.cluster._
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD


/**
  * Created by joanne on 27/12/2016.
  */
object Worker {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def runDbscan(datapoints : breeze.linalg.DenseMatrix[Double], method: String):
    Seq[(Double, Double)] = {

    //DBSCAN parameters
    val epsilon = 0.001
    val minPoints = 2

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

  def loadTweetLocations(sparkSession: SparkSession, inputPath: String):
    RDD[(Long, DenseMatrix[Double])] = {
    println(s"Loading locations from ${inputPath}...")
    sparkSession.sparkContext.objectFile(inputPath)
  }

  def extractTweetLocations(sparkSession: SparkSession, inputPath: String, outputPath: String):
    RDD[(Long, DenseMatrix[Double])] = {

    println("Extracting locations...")

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

  def loadAuthorLocations(sparkSession: SparkSession, inputPath: String):
    RDD[(Long, (Double, Double))] = {
    println(s"Loading locations from ${inputPath}...")
    sparkSession.sparkContext.objectFile(inputPath)
  }

  def extractAuthorLocations(sparkSession: SparkSession, inputPath: String, outputPath: String):
    RDD[(Long, (Double, Double))] = {

    println(s"Extracting locations from ${inputPath}...")

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

  def main(args: Array[String]) {

    val method = "median"
    val dataFile = "./data/training-1.json"
    val tweetLocationsPath = "./data/tweet_locations"
    val authorLocationsPath = "./data/author_locations"

    val sparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    // Extract tweet locations and group them by author
    val tweetLocationsRdd: RDD[(Long, DenseMatrix[Double])] =
      if (new java.io.File(tweetLocationsPath).exists)
        loadTweetLocations(sparkSession, tweetLocationsPath)
      else
        extractTweetLocations(sparkSession, dataFile, tweetLocationsPath)

    // Extract author locations
    val authorLocationsRdd: RDD[(Long, (Double, Double))] =
      if (new java.io.File(authorLocationsPath).exists)
        loadAuthorLocations(sparkSession, authorLocationsPath)
      else
        extractAuthorLocations(sparkSession, dataFile, authorLocationsPath)

    // Predict author locations from tweet locations
    val minLocations = 4
    // val clustersRdd = locationsRdd.mapValues(dbscan(_))

    // Evaluation
    val distance = Kmeans.euclideanDistance
    (authorLocationsRdd join tweetLocationsRdd)
      .filter(_._2._2.rows >= minLocations)
      .take(5)
      .foreach(entry => {
        val (actual, tweetLocations) = entry._2
        val predicted = runDbscan(tweetLocations, method)
        println("Locations:")
        println(tweetLocations)
        println("Actual:")
        println(actual)
        println("Centres:")
        println(predicted mkString "\n")

        // distance(neighbour.value, point.value)
      })


  }

}
