# scaladicious
Scala things, fails and funs.

### Project 1: Spark Twitter Language Classifier
Based on [this Databricks tutorial](https://databricks.gitbooks.io/databricks-spark-reference-applications/content/twitter_classifier/index.html). With quite a few changes for improvements and compatibility with more recent Spark versions.

To run in IntelliJ, add Twitter API credentials in `src/main/resources/twitter4j.properties`, and add this file to the sources in the project config.

### Project 2: Spark DBSCAN Algorithm
Based on [this clustering experiment](https://www.oreilly.com/ideas/clustering-geolocated-data-using-spark-and-dbscan). Some changes due to the latest ScalaNLP `nak` release not having DBSCAN yet. This also includes code that extracts and groups geotagged locations by user.

To run this, use as input file something like the `training.json` file from WNUT. Only a sample is provided here. 
