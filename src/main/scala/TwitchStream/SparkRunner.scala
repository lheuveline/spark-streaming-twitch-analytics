package TwitchStream

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/*

  TO-DO :
    * Keep wordCount DataFrame inMemory and only write new results periodically (DStream cycle interval)

 */

class SparkRunner(TwitchChannel: String, BatchDuration: Long, language: String = "en",
                   twitch_nick: String, twitch_oauth:String) {

  System.setProperty("hadoop.home.dir", "/d:/Hadoop/hadoop-3.2.1/")

  // Disable no peer warning

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.ERROR)

  // Init SparkNLP Pipeline
  val spark = SparkSession.builder()
    .appName(s"TwitchStream_$TwitchChannel")
    .master("local[*]")
    .config("spark.driver.memory", "16G")
    .config("spark.driver.maxResultSize", "0")
    .config("spark.kryoserializer.buffer.max", "2000M")
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.0")
    .config("spark.redis.host", "172.17.0.2")
    .config("spark.redis.port", "6379")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val ssc: StreamingContext = new StreamingContext(sc, Seconds(BatchDuration))

  val customReceiverStream = ssc.receiverStream(new IRCBot(TwitchChannel, twitch_nick, twitch_oauth))

  // For stream states
  ssc.checkpoint("/tmp")

  val redisInterface = new RedisInterface(spark)

  def start(): Unit = {

    val twitchChannel = ssc.sparkContext.broadcast(TwitchChannel)
    // Redis TTL config, 0 = no ttl
    val ttlMap = ssc.sparkContext.broadcast(Map(
      "rawStreamTtl" -> 1,
      "cleanStreamTtl" -> 1,
      "wordCountttl" -> 0,
      "sentimentCountTtl" -> 0, // Unused for now. Need multiLanguage model
      "categoryCountTtl" -> 0
    ))

      // Integrate streams from Worksheet

    val cleaningPipeline = pipelines.getGenericPipeline()

    // Entry stream : clean + stopwords + tokenize using spark NLP pipeline
    val mainStream = customReceiverStream.transform(rdd => {

      val _sqlContext = SparkSession.builder.getOrCreate() // Get created session
      import _sqlContext.implicits._
      val df = rdd.toDF() // => Causing serialization error, probably by sparkContext.implicits._ usage for toDF()

      val formattedDF = df.transform(helpers.formatRawDataFrame)
      val cleanDF = cleaningPipeline.fit(formattedDF).transform(formattedDF)

      cleanDF.rdd
    })
    mainStream.cache()

    // CategoryCount
    val categoryStream = mainStream.transform(rdd => {

      /* DStream to extract categories from text using TF serving */

      val _sqlContext = SparkSession.builder.getOrCreate()
      import _sqlContext.implicits._

      val df = rdd.map({
        case Row(message: String, metadata: String, timestamp: String,
        user: String, channel : String, text: String, finishedCleanText: mutable.WrappedArray[String]
        ) => (
          message, metadata, timestamp, user, channel,
          text, finishedCleanText
        )
      }).toDF("raw", "metadata", "timestamp", "user",
        "channel", "text", "finishedCleanText")

      // UDF to make request to TF Serving endpoint
      // val categoryUdf = udf(x => functions.annotateText(x))
      val categoryUdf = udf(x => TFInterface.annotateText(x))

      // Return category column as RDD
      df
        .withColumn("category", categoryUdf(col("raw")))
        .select(explode($"category").alias("category"))
        .groupBy("category")
        .count()
        .select($"category", $"count".cast(IntegerType))
        .rdd
    })
    categoryStream.cache()

    // Update CategoryCount
    val categoryCount = categoryStream
      .map{x => (x.getAs[String](0), x.getAs[Int](1))}
      .reduceByKey((x, y) => x + y)
      .updateStateByKey[Int](helpers.updateFunction)

    // WordCount and update state
    val wordCountStream = mainStream.map(row => {
      val pairs = row.getAs[mutable.WrappedArray[String]](6).toArray.map(x => (x, 1))
      pairs
    }).flatMap(x => x)
      .filter(x => x._1.length() > 3) // Filter too short words. Should be removed from mainStream in finished_clean_text
      .reduceByKey((x, y) => x + y)
      .updateStateByKey[Int](helpers.updateFunction)

    // Write WordCount to Redis
    wordCountStream.foreachRDD(rdd => {

      val _sqlContext = SparkSession.builder.getOrCreate()
      import _sqlContext.implicits._

      val df = rdd.toDF("word", "count")

      // DEV
      df.show()

      df.write.format("org.apache.spark.sql.redis")
        .option("table", s"${twitchChannel.value}" + "_wordcount")
        .option("ttl", ttlMap.value("wordCountTtl"))
        .mode(SaveMode.Overwrite).save()
    })

    // Write categoryCount to Redis
    categoryCount.foreachRDD(rdd => {

      val _sqlContext = SparkSession.builder.getOrCreate()
      import _sqlContext.implicits._

      val df = rdd.toDF("category", "count")
      val channelName = twitchChannel.value

      df.write.format("org.apache.spark.sql.redis")
        .option("table", s"$channelName" + "_categoryCount")
        .option("ttl", ttlMap.value("categoryCountTtl"))
        .mode(SaveMode.Overwrite).save()

    })

    // Stop StreamingContext
    ssc.start()
    ssc.awaitTermination()
  }
}

object helpers {

  // Running count by key.
  val updateFunction = ((newValues: Seq[Int], runningCount: Option[Int]) => {

    var result: Option[(Int)] = null

    if ( newValues.isEmpty ) { // check if the key is present in new batch if not then return the old values
      result = Some( runningCount.get )
    }
    else {
      newValues.foreach { x => { // if we have keys in new batch ,iterate over them and add it
        if ( runningCount.isEmpty ) {
          result = Some(x)// if no previous value return the new one
        } else {
          result = Some( x + runningCount.get) // update and return the value
        }
      } }
    }
    result
  })

  // Format raw message stream from Twitch IRC
  def formatRawDataFrame(df:DataFrame): DataFrame = {
    df.withColumnRenamed("value", "message")
      .withColumn("_tmp", split(col("message"), ":"))
      .select(
        col("message"),
        col("_tmp").getItem(0).alias("timestamp"),
        col("_tmp").getItem(1).alias("metadata"),
        col("_tmp").getItem(2).alias("text"),
      )
      .withColumn("_tmp", split(col("metadata"), " "))
      .select(
        col("message"),
        col("metadata"),
        col("timestamp"),
        split(col("_tmp").getItem(0), "!").getItem(0).alias("user"),
        col("_tmp").getItem(2).alias("channel"),
        lower(col("text")).alias("text")
      )
  }
}
