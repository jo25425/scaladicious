package com.databricks.apps.twitter_classifier

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.spark.mllib.linalg.Vector

/**
  * Created by joanne on 20/12/2016.
  *
  * Pulls live tweets and filters them for tweets in the chosen cluster.
  */
object Predict {
  private val gson = new Gson()

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]) {

    val intervalInSeconds = 5
    val clusterNumber = 3
    val modelFile = "./tweets/model"

    val sparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext

    val streamingContext = new StreamingContext(sparkContext, Seconds(intervalInSeconds))

    // Set up Twitter stream
    def twitterAuth: Some[OAuthAuthorization] = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))

    val tweetStream = TwitterUtils
      .createStream(streamingContext, twitterAuth)

    val texts = tweetStream.map(_.getText) // Using .getText avoids having to parse from gson to json

    println("--- Initializing K-Means model ---")
    val model = new KMeansModel(sparkContext.objectFile[Vector](modelFile.toString).collect())
    val numFeatures = model.clusterCenters(0).size
    println(s"Number of features for model: $numFeatures")

    val filteredTweets = texts
      .filter(t => model.predict(Utils.vectorize_one(Utils.tokenize(t), numFeatures)) == clusterNumber)
    filteredTweets.print()

     // Start the streaming computation
    println("Initialization complete.")
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
