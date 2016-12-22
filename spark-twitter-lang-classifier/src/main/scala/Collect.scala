package com.databricks.apps.twitter_classifier

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

/**
  *
  * Created by joanne on 14/12/2016.
  *
  * Collect at least the specified number of tweets into json text files.
  */
object Collect {
  private var numTweetsCollected = 0L
  private val gson = new Gson()

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  def main(args: Array[String]) {

    // Process arguments
    val outputDirectory = "./tweets" // the output directory for writing the tweets. The files will be named 'part-%05d'
    val numTweetsToCollect = 10000 // this is the minimum number of tweets to collect before the program exits.
    val intervalInSeconds = 10 // write out a new set of tweets every interval.
    val partitionsEachInterval = 5


    // Spark streaming
    val sparkConfig = new SparkConf()
      .setMaster("local[4]") // Square brackets for port
      .setAppName(this.getClass.getSimpleName)

    val sparkContext = new SparkContext(sparkConfig)

    val streamingContext = new StreamingContext(sparkContext, Seconds(intervalInSeconds))

    // Twitter auth & stream setup
    def twitterAuth: Some[OAuthAuthorization] = Some(new OAuthAuthorization(new ConfigurationBuilder().build))
    val tweetStream = TwitterUtils
      .createStream(streamingContext, twitterAuth)
      .map(gson.toJson(_))

    // Define tweet stream processing
    tweetStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(partitionsEachInterval)
        outputRDD.saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
        }
      }
    })

    // Go!
    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
