import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object rec {
  def main(args:Array[String]): Unit = {

    val t0 = System.nanoTime()

    // configure spark environment
    val sparkConf = new SparkConf().setAppName("rec").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    /*
    Pre-process the data
     */

    def flatGenre(id:Int, genreList:Array[String]): Array[(Int, String)] = {
      /*
      This function is to be used in flatMap method, 
      to flat RDD of (movieId, Array(genre)) into RDD of (movieId, genre)
      */
      genreList.map(genre => (id, genre))
    }
    
    // load inputs
    val ratings = sc.textFile(args(0))
    val testing = sc.textFile(args(1))
    val movies = sc.textFile(args(2))

    // transform to RDD with useful columns (also remove the headers)
    val ratingsRDD = ratings.mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}
      .map(line => line.split(",")).map(line => ((line(0).toInt, line(1).toInt), line(2).toDouble))
    // ((userId, movieId), rating)
    val testingRDD = testing.mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}
      .map(line => line.split(",")).map(line => ((line(0).toInt, line(1).toInt), 1))
    // ((userId, movieId), 1)
    
    val moviesRDD = movies.mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}
      .map(line => line.split(","))
      .map(line => (line(0).toInt, line.last.split("""\|"""))) // (movieId, Array(genre))
      .flatMap(line => flatGenre(line._1, line._2)) // (movieId, genre)

    // remove the ratings of testing data points from ratingsRDD
    // and add them to the testingRDD
    val joined = ratingsRDD.leftOuterJoin(testingRDD)
    // if a data point is in testset, then the joined line would be ((uid, mid), (rating, 1)), else ((uid, mid), (rating, None))
    // split training and test data based on whether the fourth element is None
    val trainingRDD = joined.filter(line => line._2._2.isEmpty).map(line => (line._1, line._2._1))
    val testRDD = joined.filter(line => line._2._2.isDefined).map(line => (line._1, line._2._1))
    // both of ((userId, movieId), rating)
    //println(trainingRDD.count()) //- 79748
    //println(testRDD.count()) //- 20256
    //trainingRDD.take(10).foreach(println)
    //testingRDD.take(10).foreach(println)

    // calculate average rating of each users
    val userAvgRating = trainingRDD.map(line => (line._1._1, line._2)).groupByKey().map(line => (line._1, line._2.toList.sum/line._2.toList.length))
    // (userId, avgRating)


    /*
    Task 2: User-based CF Algorithm
     */

    /*
    Helper functions
     */

    def findCommonUsers(movie1:Iterable[(Int, Double)], movie2:Iterable[(Int, Double)]): Iterable[(Double, Double)] = {
      /*
      This function takes the two (user, rating) iterables of two movies, 
      to figure out the common users who have rated the two,
      and return the iterable of (ratingOfMovie1, ratingOfMovie2) pairs for each common user
      */
      val movieMap1 = movie1.toMap
      val movieMap2 = movie2.toMap
      var commonUsers = Map[Int, (Double, Double)]()

      for (user <- movieMap1.keys) {
        if (movieMap2.contains(user)) {
          commonUsers += (movie -> (movieMap1(user), movieMap2(user)))
        }
      }

      commonUsers.values
    }


    def calCorr(array1:Array[Double], array2:Array[Double], length:Int): Double ={
      /*
      This function is to calculate the Pearson correlation between two rating arrays
       */
      val avg1 = array1.sum / array1.length
      val avg2 = array2.sum / array2.length
      var sumxy = 0.0
      var sumx2 = 0.0
      var sumy2 = 0.0

      for (i <- 0 until length) {
        sumxy += (array1(i)-avg1) * (array2(i)-avg2)
        sumx2 += (array1(i)-avg1) * (array1(i)-avg1)
        sumy2 += (array2(i)-avg2) * (array2(i)-avg2)
      }

      if (sumx2 == 0 || sumy2 == 0) 0.0
      else sumxy / math.sqrt(sumx2) / math.sqrt(sumy2)

    }


    def flattenPairs(userTarget:Int, movieTarget:Int, similarUser:Array[(Int, Double, Double)]): Array[(Int, Int, Int, Double, Double)] ={
      /*
      This function is to flatten the ((userTarget, movieTarget), Array(topSimilarUser, corr, rating)))
      to Array(userTarget, similarUser1, movieTarget, corr)
       */
      similarUser.map(line => (userTarget, line._1, movieTarget, line._2, line._3))
    }


    def userBasedPrediction(userAvg: Double, similarUsers: Iterable[(Int, Double, Double, Double)]): Double ={
      /*
      This function is to calculate predicted rating following the user-based CF algorithm
      Input: userAvg, Iterable(similarUser1, corr, rating, similarUserAvgRating)
      Output: rating
       */

      var numerator = 0.0
      var denominator = 0.0

      for (user <- similarUsers) {
        // calculate the average rating on rated items other than the target one
        //val newUserAvg:Double = (user._4 * user._5 - user._3) / (user._5 - 1)
        //numerator += user._2 * (user._3 - newUserAvg)
        numerator += user._2 * (user._3 - user._4)
        denominator += math.abs(user._2)
      }

      if (denominator != 0) userAvg + numerator / denominator
      else userAvg
    }


    def scoreValidation(score:Double): Double = {
      /*
      This function is to turn predictions lower than 0 to 0.5, and higher than 5 to 5.0
       */

      if (score < 0) 0.5
      else if (score > 5) 5.0
      else score
    }


    /*
    User-based Recommender
     */


    // when doing user-based recommendation, consider users who have rated at least 30 movies only
    val frequentUsers = trainingRDD.map(_._1) // (userId, movieId))
      .groupByKey()
      .filter(_._2.toArray.length >= 30)
      .map(line =>(line._1, 1)) //(userId, 1)


    val movieUser = trainingRDD.map(line => (line._1._1, (line._1._2, line._2))) // (userid, (movieid, rating))
      .join(frequentUsers) // (userid, ((movieid, rating),1))
      .map(line => (line._1, line._2._1))
      .groupByKey() // (userid, iterable(movieid, rating))
      .map(line => (1, line))

    val userCorr = movieUser.join(movieUser) // (1, ((uid1, iter(mid, rating)), (uid2, iter(mid, rating)))
      .map(line => ((line._2._1._1, line._2._2._1), findCommonUsers(line._2._1._2, line._2._2._2))) // ((user1, user2), Iterable(rating1, rating2))
      .filter(line => line._1._1 != line._1._2 && line._2.toArray.length >= 10) // remove duplicates and consider pairs co-rated more than 10 movies only  
      .map(line => (line._1, (line._2.toArray.map(_._1), line._2.toArray.map(_._2)))) // ((user1, user2), (array(rating1), array(rating2))) 
      .map(line => (line._1, calCorr(line._2._1, line._2._2, line._2._1.length))) // ((user1, user2), corr)
      .map(line => (line._1._1, (line._1._2, line._2))) // (user1, (user2, corr))

    // make predictions
    val predictionsUserBased = testRDD.map(_._1) // (userId, movieId)
      .join(userCorr) // (userTarget, (movieTarget, (anotherUser, corr)))
      .map {case (userTarget, (movieTarget, (anotherUser, corr))) => ((anotherUser, movieTarget), (userTarget, corr))}
      .join(trainingRDD) // ((anotherUser, movieTarget), ((userTarget, corr), rating))
      .map {case ((anotherUser, movieTarget), ((userTarget, corr), rating)) => (anotherUser, (userTarget, movieTarget, corr, rating))}
      .join(userAvgRating) // (similarUser1, ((userTarget, movieTarget, corr, rating), similarUserAvgRating))
      .map {case (similarUser1, ((userTarget, movieTarget, corr, rating), similarUserAvgRating)) => (userTarget, (similarUser1, movieTarget, corr, rating, similarUserAvgRating))}
      .join(userAvgRating) // (userTarget, ((similarUser1, movieTarget, corr, rating, similarUserAvgRating), userTargetAvgRating))
      .map {case (userTarget, ((similarUser1, movieTarget, corr, rating, similarUserAvgRating), userTargetAvgRating)) => ((userTarget, movieTarget, userTargetAvgRating), (similarUser1, corr, rating, similarUserAvgRating))}
      .groupByKey() // ((userTarget, movieTarget, userTargetAvgRating), Iterable(similarUser1, corr, rating, similarUserAvgRating))
      .map(line => ((line._1._1, line._1._2), userBasedPrediction(line._1._3, line._2)))
      .map(line => (line._1, scoreValidation(line._2))) // ((userId, movieId), validatedScore)

    // find all unpredicted pairs
    //val unpredicted = testingRDD.leftOuterJoin(predictionsUserBased)
    //  .filter(line => line._2._2.isEmpty)
    //  .map(line => (line._1._2, line._1._1)) // (movieId, userId)
    
    val allToPredict = testingRDD.map(line => line._1.swap) // (movieId, userId)
    
    // average rating of the genre of the user
    val genreUserAvg = trainingRDD.map(line => (line._1._2, (line._1._1, line._2))) // (movieId, (userId, rating))
      .join(moviesRDD) // (movieId, ((userId, rating), genre))
      .map(line => ((line._2._1._1, line._2._2), (line._2._1._2, 1))) // ((userId, genre), (rating,1))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .map(line => (line._1, (line._2._1 / line._2._2, line._2._2))) // ((userId, genre), (avgRating, count))

    def weightedAvg(iter:Iterable[(Double, Int)]):Double = {
      var numerator = 0.0
      var denominator = 0.0
      
      for (pair <- iter) {
        numerator += pair._1 * pair._2
        denominator += pair._2
      }
      
      numerator / denominator
    }

    val filled1 = allToPredict
      .join(moviesRDD) // (movieId, (userId, genre))
      .map(line => (line._2, line._1)) // ((userId, gerne), movieId)
      .join(genreUserAvg) // ((userId, genre), (movieId, (avgRating, count)))
      .map(line => ((line._1._1, line._2._1), line._2._2)) // ((userId, movieId), (avgRating, count))
      .groupByKey()
      .map(line => (line._1, weightedAvg(line._2))) // ((userId, movieId), weightedGenreAvg)
    
    // average rating of the movie
    val movieAverage = trainingRDD.map(line => (line._1._2, line._2)) // (movieId, rating)
      .groupByKey()
      .map(line => (line._1, line._2.toArray.sum/line._2.toArray.length, line._2.toArray.length)) // (movieId, avgRating, ratingCount)
      .map(line => (line._1, line._2)) // (movieId, avgRating)

    val filled2 = allToPredict
      .join(movieAverage) // (movieId, (userId, avgRating))
      .map(line => ((line._2._1, line._1), line._2._2)) // ((userId, movieId), avgRating)

    // average rating of the users
    val filled3 = allToPredict
      .map(line => (line._2, line._1))
      .join(userAvgRating) // (userId, (movieId, avgRating))
      .map(line => ((line._1, line._2._1), line._2._2)) // ((userId, movieId), avgRating)

    // simply make the prediction by taking the average of User-based CF prediction, User Genre Average, Movie Average and User Average
    val allPredictions = filled1.union(filled2).union(filled3).union(predictionsUserBased)
      .groupByKey()
      .map(line => (line._1, line._2.sum/line._2.toArray.length))
    
    val all = allPredictions.sortByKey()
    // save the predictions to csv
    val header: RDD[String] = sc.parallelize(Array("UserId,MovieId,Pred_rating"))
    val output = all.map(row => row._1._1 + "," + row._1._2 + "," + row._2)
    header.union(output).coalesce(1).saveAsTextFile("./predictions")

    /*
    Evaluation
    */

    // count ratings in different intervals
    val zeroToOne = all.filter(line => line._2 >= 0 && line._2 < 1).count()
    val oneToTwo = all.filter(line => line._2 >= 1 && line._2 < 2).count()
    val twoToThree = all.filter(line => line._2 >= 2 && line._2 < 3).count()
    val threeToFour = all.filter(line => line._2 >= 3 && line._2 < 4).count()
    val fourAbove = all.filter(line => line._2 >= 4).count()

    // calculate RMSE
    val ActualAndPred = testRDD.join(all)
    val MSE = ActualAndPred.map {case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    val RMSE = math.sqrt(MSE)

    val t1 = System.nanoTime()

    // print all the summary statistics
    val summary:String =  ">=0 and <1: " + zeroToOne + "\n" +
      ">=1 and <2: " + oneToTwo + "\n" +
      ">=2 and <3: " + twoToThree + "\n" +
      ">=3 and <4: " + threeToFour + "\n" +
      ">=4: " + fourAbove + "\n" +
      "RMSE = " + RMSE + "\n" +
      "The total execution time taken is "+((t1-t0) / 1000000000.0) + " sec."

    println(summary)

  }
}
