package com.databricks.apps.twitter_classifier

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
  * Created by joanne on 18/12/2016.
  */
object Utils {

  def tokenize(document: String): Seq[String] = {
    document.split(" ").map(_.toLowerCase).toSeq
  }

  /**
    * Create feature vectors by turning each tweet into bigrams of
    * characters (an n-gram model) and then hashing those to a
    * length-1000 feature vector that we can pass to MLlib.
    * This is a common way to decrease the number of features in a
    * model while still getting excellent accuracy. (Otherwise every
    * pair of Unicode characters would potentially be a feature.)
    */

  def vectorize_batch(documents: RDD[Seq[String]], numFeatures: Int): RDD[Vector] = {
    val tf = new HashingTF(numFeatures)

    val vectors: RDD[Vector] = tf.transform(documents)
    vectors.cache()
    vectors.count()
    return vectors
  }

  def vectorize_one(document: Seq[String], numFeatures: Int): Vector = {
    val tf = new HashingTF(numFeatures)
    val vector: Vector = tf.transform(document)
    return vector
  }

}
