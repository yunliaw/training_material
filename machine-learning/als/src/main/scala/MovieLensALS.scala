import java.io.File

import scala.io.Source

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

object MovieLensALS {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)



    // set up environment

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("MovieLensALS")
    val sc = new SparkContext(conf)

    // load personal ratings

    val myRatings = loadRatings("../personalRatings.txt")
    val myRatingsRDD = sc.parallelize(myRatings, 1)

    // load ratings and movie titles

    val movieLensHomeDir = "../../data"

    val ratings = sc.textFile(new File(movieLensHomeDir, "ratings.dat").toString).map { line =>
      val fields = line.split("::")
      // format: (timestamp % 10, Rating(userId, movieId, rating))
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }

    val movies = sc.textFile(new File(movieLensHomeDir, "movies.dat").toString).map { line =>
      val fields = line.split("::")
      // format: (movieId, movieName)
      (fields(0).toInt, fields(1))
    }.collect().toMap

    // your code here
    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2).distinct.count()
    val numMovies = ratings.map(_._2.product).distinct.count()
    println (s"numRatings: $numRatings")
    println(s"numUsers: $numUsers")
    println(s"numMovies: $numMovies")

    val numPartitions = 4

    val training = ratings.filter(x => x._1 < 6)
      .values.union(myRatingsRDD).repartition(numPartitions).cache()
    println("training count: " + training.count())

    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values.repartition(numPartitions).cache()
    println("validation count: " + validation.count())

    val test = ratings.filter(x => x._1 >= 8).values.cache()
    println("test count: " + test.count())


    val ranks = List(8, 12)
    val lambdas = List(0.1, 1.0, 10.0)
    val numIters = List(10, 20)

    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1

//    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
//      val model = ALS.train(training,rank,numIter,lambda)
//      val validationRmse = computeRmse(model, validation, validation.count())
//      println(s"RMSE (validation) = $validationRmse for model rank = $rank, lambda = $lambda, numIter = $numIter")
//      if (validationRmse < bestValidationRmse) {
//        bestModel = Some(model)
//        bestValidationRmse = validationRmse
//        bestRank = rank
//        bestLambda = lambda
//        bestNumIter = numIter
//      }
//    }
    bestModel = Some(ALS.train(training, 12, 10, 0.1))
    bestRank = 12
    bestNumIter = 10
    bestLambda = 0.1
    val testRmse = computeRmse(bestModel.get, test, test.count())
    println(s"RMSE (test) = $testRmse for model rank = $bestRank, lambda = $bestLambda, numIter = $bestNumIter")



    val meanRating = training.union(validation).map(_.rating).mean
    val baselineRmse =
      math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean())
    val improvement =  (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    val myRatedMovieIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recomendations = bestModel.get
      .predict(candidates.map((0, _)))
      .collect()
      .sortBy(- _.rating)
      .take(50)

    var i = 1
    println("Movies recommended:")
    recomendations.foreach {
      r => println("%2d".format(i) + ": " + movies(r.product))
        i += 1
    }
    // clean up
    sc.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  /** Load ratings from file. */
  def loadRatings(path: String): Seq[Rating] = {
    val lines = Source.fromFile(path).getLines()
    val ratings = lines.map { line =>
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.filter(_.rating > 0.0)
    if (ratings.isEmpty) {
      sys.error("No ratings provided.")
    } else {
      ratings.toSeq
    }
  }
}
