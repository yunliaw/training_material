package streaming

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import com.google.gson.Gson
import java.io.File

object Collect {

  private val gson = new Gson()
  private var numTweetsCollected = 0L
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // Configure Twitter credentials
    val apiKey = ""
    val apiSecret = ""
    val accessToken = ""
    val accessTokenSecret = ""
    TutorialHelper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)

    // Create Output Dir
    val outputDirPath = "/tmp/tweets"
    val outputDir = new File(outputDirPath)
    if (outputDir.exists()) {
      System.err.println("ERROR - %s already exists: delete or specify another directory".format(
        outputDirPath))
      System.exit(1)
    }
    outputDir.mkdirs()

    // Collect 10000 rows of data
    val numTweetsToCollect = 10000L

    println("---start streaming")
    val ssc = new StreamingContext(new SparkConf().setMaster("local[2]").setAppName("Collect"), Seconds(5))

    val tweets = TwitterUtils.createStream(ssc, None) map (gson.toJson(_)) filter(!_.contains("boundingBoxCoordinates"))

    tweets.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(1)
        outputRDD.saveAsTextFile(outputDirPath + "/tweets_" + time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

