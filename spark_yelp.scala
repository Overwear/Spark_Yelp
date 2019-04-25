//spark-shell --executor-memory 8G --num-executors 4 --executor-cores 8
//val file = sc.textFile("hdfs:/user/lee48493/project/yelp_academic_dataset_review.json")
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

	def main(args: Array[String]) 
	{
		if (args.length < 3) 
		{
			println("Usage: SparkYelp <input> <output> <numOutputFiles>")
			System.exit(1)
		}
		val sparkConf = new SparkConf().setAppName("Spark Yelp")
		val sc = new SparkContext(sparkConf)
		val data = sc.textFile(args(0))

		//Average Star Review Across Entire DataSet
		val star_data = data.map(entry => entry.split(",")(STARS))
							.map(x => x.split(":")(RATING_VALUE).toFloat)

		val star_data_total = star_data.reduce(_+_)	
		val num_of_star_reviews = star_data.count
		val average_star_review = star_data_total/num_of_star_reviews
		println(average_star_review)

		//List of stop words
		//source: https://www.geeksforgeeks.org/removing-stop-words-nltk-python/
		val stop_words = List("a","about","above","after","again","against","all","am","an","and","any","are",
								"aren't","as","at","be","because","been","before","being","below","between","both",
								"but","by","can't","cannot","could","couldn't","did","didn't","do","does","doesn't",
								"doing","don't","down","during","each","few","for","from","further","had","hadn't","has",
								"hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's",
								"hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've",
								"if","in","into","is","isn't","it","it's","its","itself","let's","me","more","most",
								"mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other",
								"ought","our","ours","ourselves","out","over","own","same","shan't","she","she'd",
								"she'll","she's","should","shouldn't","so","some","such","than","that","that's","the",
								"their","theirs","them","themselves","then","there","there's","these","they","they'd",
								"they'll","they're","they've","this","those","through","to","too","under","until","up",
								"very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's",
								"when","when's","where","where's","which","while","who","who's","whom","why","why's",
								"with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your",
								"yours","yourself","yourselves")
		
		//Prepping the data for word count
		val data_prep = data.map(entry => entry.split(",")(TEXT))
							.map(x => x.split(":")(TEXT_INFO))
							.filter(_.length >= 2)
							.flatMap(_.split(" "))
							.filter(_.length > 2)
							.map(x => x.replaceAll("""[\p{Punct}]""", ""))

		//Word count
		//sc.parallelize(file.map(_.split(",")(7)).map(_.split(":")(1)).filter(_.size > 1).flatMap(_.split(" ")).filter(_.length > 2).map(_.replaceAll("""[\p{Punct}]""", "")).map(x => (x.toLowerCase,1)).reduceByKey(_+_,1).top(100)(Ordering.by(x => x._2)), 1).saveAsTextFile("top_100_words_on_yelp")
		val total_word_count = data_prep.map(x => (x.toLowerCase,1))
										.reduceByKey(_+_, NUM_OF_PARTS)
										.top(100)(Ordering.by(x => x._2))

		//Word count with stop words removed
		//sc.parallelize(file.map(_.split(",")(7)).map(_.split(":")(1)).filter(_.size > 1).flatMap(_.split(" ")).filter(_.length > 2).map(_.replaceAll("""[\p{Punct}]""", "")).map(x => x.toLowerCase).filter(x => !stop_words.contains(x)).map(x => (x,1)).reduceByKey(_+_,1).top(100)(Ordering.by(x => x._2)), 1).take(20)
		val sw_total_word_count = data_prep.map(x => x.toLowerCase)
											.filter(x => !stop_words.contains(x))
											.map(x => (x,1))
											.reduceByKey(_+_, NUM_OF_PARTS)
											.top(100)(Ordering.by(x => x._2))

		//Savefiles
		sc.parallelize(total_word_count, NUM_OF_PARTS).saveAsTextFile(args(1) + "/word_count")
		sc.parallelize(sw_total_word_count, NUM_OF_PARTS).saveAsTextFile(args(1) + "/stopwords_word_count")
		System.exit(0)
	}
}
