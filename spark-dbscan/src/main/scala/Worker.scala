import java.io.File

import breeze.linalg.DenseMatrix
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import nak.cluster._
import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD


/**
  * Created by joanne on 27/12/2016.
  */
object Worker {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def dbscan(v : breeze.linalg.DenseMatrix[Double]): Unit = {
    //DBSCAN parameters
    val epsilon = 0.001
    val minPoints = 2

    val gdbscan = new GDBSCAN(
      DBSCAN.getNeighbours(epsilon = epsilon, distance = Kmeans.euclideanDistance),
      DBSCAN.isCorePoint(minPoints = minPoints)
    )
    val clusters = gdbscan cluster v
    return clusters
  }

  def prepareLocations(sparkSession: SparkSession, inputPath: String, outputPath: String):
                       RDD[(Long, DenseMatrix[Double])] = {
    val tweets = sparkSession.read.json(inputPath)

    // Group locations per user in a breeze DenseMatrix
    val locationsPerUser = tweets.rdd
      .map(loc => (
        loc.getAs[String]("user_id"),
        loc.getAs[String]("tweet_latitude"),
        loc.getAs[String]("tweet_longitude")
      ))
      .groupBy(_._1.toLong)
      .mapValues(userTweets => {
        val (numRows, numCols) = (userTweets.size, 2)
        val latitudes = userTweets.map(_._2.toDouble)
        val longitudes = userTweets.map(_._3.toDouble)
        val values = (latitudes ++ longitudes).toArray
        new DenseMatrix(numRows, numCols, values)
      })

    // Check results
//    locationsPerUser.take(10).foreach(u => {
//      println(s"\n${u._1} (${u._2.rows} x ${u._2.cols}):")
//      println(u._2)
//    })

    // Save locations by user
    FileUtils.deleteQuietly(new File(outputPath)) // Delete if already exists
    locationsPerUser.saveAsObjectFile(outputPath)

    locationsPerUser
  }

  def main(args: Array[String]) {

    val dataFile = "./data/training-1.json"
    val userLocationsPath = "./data/user_locations"

    val sparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    // Extract locations and group them by author
    val locationsRdd = prepareLocations(sparkSession, dataFile, userLocationsPath)

    // Check results
    //    locationsPerUser.take(10).foreach(u => {
    //      println(s"\n${u._1} (${u._2.rows} x ${u._2.cols}):")
    //      println(u._2)
    //    })

    locationsRdd.take(100).foreach(userLocations => {
      val l = userLocations._2
      if (l.rows > 1) {
        println("Locations:")
        println(l)
        val c = dbscan(l)
        println("Clusters:")
        println(c)
      }
    })

    //    val clustersRdd = locationsRdd.mapValues(dbscan(_))

  }

}
