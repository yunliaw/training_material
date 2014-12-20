package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg.Vector



object Predict {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // Configure Twitter credentials
    val apiKey = "umhLxAaxpcfQrAksVfwl8rcKw"
    val apiSecret = "yAD1GZecWpFWGSd0kehGMweToRDNAW1xaGsHzi3bObMagLkyFu"
    val accessToken = "22887707-Sc0SMMJNd7EggYCXMyq4OfQl5V1ZdCCHSX2UR4BtV"
    val accessTokenSecret = "SbHsQou18OVzYlFrDUbaBdohJtzwPxCAL6ZCsOw"
    TutorialHelper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)

    val modelFile = "/tmp/tweets/model"
    val clusterNumber = 2

    val conf = new SparkConf().setMaster("local[2]").setAppName("Predict")
    val ssc = new StreamingContext(conf, Seconds(5))

    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(_.getText)

    val model = new KMeansModel(ssc.sparkContext.objectFile[Vector](modelFile).collect())

    val filteredTweets = statuses
      .filter{t => model.predict(TutorialHelper.featurize(t)) == clusterNumber}
    filteredTweets.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
