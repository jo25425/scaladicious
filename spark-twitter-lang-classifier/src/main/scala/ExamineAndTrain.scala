package com.databricks.apps.twitter_classifier

import java.io.File
import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by joanne on 15/12/2016.
  *
  * Examine the collected tweets and trains a model based on them.
  */
object ExamineAndTrain {
  val jsonParser = new JsonParser()
  val gson = new GsonBuilder().setPrettyPrinting().create()

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  def main(args: Array[String]) {

    // Process arguments
    val tweetInput = "./tweets/tweets_1482238460000" // /tmp/tweets/tweets*
    val outputModelDir = "./tweets/model" // /tmp/tweets/model
    val numFeatures = 10000
    val numIterations = 20
    var numClusters = 10


    val sparkSession = SparkSession.builder
      .master("local[4]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    // Spark SQL can load JSON files and infer the schema based on that data
    val tweets = sparkSession.read.json("./tweets/tweets_*")
    tweets.createOrReplaceTempView("tweetTable")
    tweets.cache()

    println("\n--- Tweet table Schema ---")
    tweets.printSchema()

    // View the user language, user name, and text for 10 sample tweets.
    println("\n--- Sample Lang, Name, text ---")
    sparkSession
      .sql("SELECT user.lang, user.name, text FROM tweetTable LIMIT 10")
      .collect().foreach(println)

    // Another way of doing this:
    //    sparkSession
    //      .sql("SELECT text FROM tweetTable")
    //      .show(10)

    // Finally, show the count of tweets by user language.
    // This can help determine the number of clusters that is ideal for this dataset of tweets.
    println("\n--- Total count by languages Lang, count(*) ---")
    var numLanguages = 0
    sparkSession
      .sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable " +
           "GROUP BY user.lang ORDER BY cnt DESC")
      .collect().foreach(l => {
        println(l)
        numLanguages += 1
      })
    println(s"Found ${numLanguages.toString()} languages.")

    // This can help determine the number of clusters that is ideal for this dataset of tweets.
    numClusters = numLanguages


    println("\n--- Train the model and persist it ---")

    val sparkContext = sparkSession.sparkContext

//    val texts = sparkSession.sql("SELECT text FROM tweetTable").rdd.map(
//      _.mkString.split(" ")
//      .map(_.toLowerCase)
//      .toSeq
//    )

//    val vectors: RDD[Vector] = hashingTF.transform(texts)
//    vectors.cache() // Caches the vectors since it will be used many times by KMeans.
//    vectors.count()  // Calls an action on the RDD to populate the vectors cache.
//
//    val model = KMeans.train(vectors, numClusters, numIterations)
//    FileUtils.deleteQuietly(new File(outputModelDir)) // Delete if already exists
//    sparkContext.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(outputModelDir)
//
//    println("----Example tweets from the clusters")
//    val some_tweets = texts.take(100)
//    for (i <- 0 until numClusters) {
//      println(s"\nCLUSTER $i:")
//      some_tweets.foreach { t =>
//        if (model.predict(hashingTF.transform(t)) == i) {
//          println(t)
//        }
//      }
//    }

    // EXPERIMENT TIME!!

    val pairs = sparkSession.sql("SELECT text, user.lang AS lang FROM tweetTable").rdd.map {
      case Row(text: String, lang: String) => (Utils.tokenize(text.mkString), lang)
    }

    val vectors2 = Utils.vectorize_batch(pairs.map(_._1), numFeatures)

    val model2 = KMeans.train(vectors2, numClusters, numIterations)
    FileUtils.deleteQuietly(new File(outputModelDir)) // Delete if already exists
    sparkContext.makeRDD(model2.clusterCenters, numClusters).saveAsObjectFile(outputModelDir)


    println("\n--- Example tweets from the clusters ---")
    val somePairs = pairs.take(pairs.count().toInt)
    for (i <- 0 until numClusters) {
      val counts = somePairs
        .filter(t => model2.predict(Utils.vectorize_one(t._1, numFeatures)) == i)
        .map(_._2)
        .groupBy(identity)
        .mapValues(_.size)
      println(s"\nCLUSTER $i: $counts")
    }

    //TODO: Print cluster languages in descending order of frequency
    //TODO: Define a metric for evaluating clusters quality

  }

}
