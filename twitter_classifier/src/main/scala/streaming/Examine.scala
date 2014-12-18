package streaming

import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Examine {
  val jsonParser = new JsonParser()
  val gson = new GsonBuilder().setPrettyPrinting().create()

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val tweetInput = "/tmp/tweets/tweets*/part-*"
    val outputModelDir = "/tmp/tweets/model"
    val numClusters = 10
    val numIterations = 20

    val conf = new SparkConf().setMaster("local[2]").setAppName("Examine")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


//    val tweets = sc.textFile(tweetInput)
//    println("------------Sample JSON Tweets-------")
//    tweets.take(1) map { tweet =>
//      println(gson.toJson(jsonParser.parse(tweet)))
//    }

    val tweetTable = sqlContext.jsonFile(tweetInput).cache()
    tweetTable.registerTempTable("tweetTable")
//
//    println("------Tweet table Schema---")
//    tweetTable.printSchema()
//
//    println("----Sample Tweet Text-----")
//    sqlContext.sql("SELECT text FROM tweetTable LIMIT 10").collect().foreach(println)
//
//    println("------Sample Lang, Name, text---")
//    sqlContext.sql("SELECT user.lang, user.name, text FROM tweetTable LIMIT 10").collect().foreach(println)
//
//    println("------Total count by languages Lang, count(*)---")
//    sqlContext.sql("SELECT user.lang, COUNT(*) as cnt FROM tweetTable GROUP BY user.lang ORDER BY cnt DESC LIMIT 25").collect.foreach(println)

    println("--- Training the model and persist it")
    val texts = sqlContext.sql("SELECT text from tweetTable") map (_.head.toString)

    // Caches the vectors since it will be used many times by KMeans.
    val vectors = (texts map TutorialHelper.featurize).cache()

    // Calls an action on the RDD to populate the vectors cache.
    vectors.count()

    val model = KMeans.train(vectors, numClusters, 30)
    val cost = model.computeCost(vectors)
    println(s"cost: $cost")
    //sc.makeRDD(model.clusterCenters, numClusters).saveAsObjectFile(outputModelDir)


//    val some_tweets = texts.take(10)
//    println("----Example tweets from the clusters")
//    for (i <- 0 until numClusters) {
//      println(s"\nCLUSTER $i:")
//      some_tweets.foreach { t =>
//        if (model.predict(TutorialHelper.featurize(t)) == i) {
//          println(t)
//        }
//      }
//    }
  }
}
