//val file = sc.textFile("hdfs:/user/lee48493/project/academic_dataset_review.json")
//file.map(_.split(",")(7)).map(_.split(":")(1)).take(1)

//average star rating
//word count
//Filter useful reviews and do a word count
//Filter funny reviews and do a word count
//Filter cool reviews and do a word count

import org.apache.spark.{ SparkConf, SparkContext }
object SparkYelp 
{
	val REVIEW_ID: Int = 	0
	val USER_ID: Int = 		1
	val BUSINESS_ID: Int = 	2
	val STARS: Int = 		3
	val USEFUL: Int = 		4
	val FUNNY: Int = 		5
	val COOL: Int = 		6
	val TEXT: Int = 		7
	val DATE: Int = 		8
	val RATING_VALUE =		1
	val TEXT_INFO =			1
	val NUM_OF_PARTS =		1
	var sc: SparkContext = _
	def main(args: Array[String]) 
	{
		if (args.length < 3) 
		{
			println("Usage: SparkYelp <input> <output> <numOutputFiles>")
			System.exit(1)
		}
		val sparkConf = new SparkConf().setAppName("Spark Yelp")
		val sc = new SparkContext(sparkConf)
		val data = sc.textFile(args(0)).map(entry => entry.split(","))

		//Average Star Review Across Entire DataSet
		val star_data = data(STARS).map(x => x.split(":")(RATING_VALUE).toFloat)
		val star_data_total = star_data.reduce(_+_)	
		val num_of_star_reviews = star_data.count
		val average_star_review = star_data_total/num_of_star_reviews
		average_star_review.saveAsTextFile(args(1))

		//Word count
		//sc.parallelize(file.map(_.split(",")(7)).map(_.split(":")(1)).filter(_.size > 1).flatMap(_.split(" ")).filter(_.length > 2).map(_.replaceAll("""[\p{Punct}]""", "")).map(x => (x.toLowerCase,1)).reduceByKey(_+_,1).top(100)(Ordering.by(x => x._2))).saveAsTextFile("top_100_words_on_yelp")

		val total_word_count = data(TEXT).map(x => x.split(":")(TEXT_INFO))
								.filter(_.length >= 2)
								.flatMap(_.split(" "))
								.filter(_.length > 2)
								.map(x => x.replaceAll("""[\p{Punct}]""", ""))
								.map(x => (x.toLowerCase,1))
								.reduceByKey(_+_, NUM_OF_PARTS)
								.top(100)(Ordering.by(x => x._2))


		

			.filter(_.size > 5)
			.map(x => (x(STOCK_SYMBOL), (x(HIGH_PRICE).toFloat - x(LOW_PRICE).toFloat) * 100 / x(LOW_PRICE).toFloat))
			.reduceByKey(Math.max(_, _), args(2).toInt)
			.sortByKey(ascending = true)
			.saveAsTextFile(args(1))

		System.exit(0)}}
	}
}
