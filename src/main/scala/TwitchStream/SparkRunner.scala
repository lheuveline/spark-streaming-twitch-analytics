package TwitchStream

import com.johnsnowlabs.nlp.annotators._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher, SparkNLP}
import org.apache.spark._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._

class SparkRunner(
                   TwitchChannel: String,
                   BatchDuration: Int,
                   language: String = "en",
                   twitch_nick: String,
                   twitch_oauth:String) {

  System.setProperty("hadoop.home.dir", "/d:/Hadoop/hadoop-3.2.1/")

  // Disable no peer warning
  import org.apache.log4j.{Level, Logger}
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Init SparkNLP Pipeline
  val spark = SparkSession.builder()
    .appName(s"TwitchStream_$TwitchChannel")
    .master("local[*]")
    .config("spark.driver.memory","16G")
    .config("spark.driver.maxResultSize", "0")
    .config("spark.kryoserializer.buffer.max", "2000M")
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.0.0")
    .config("spark.redis.host", "172.17.0.2")
    .config("spark.redis.port", "6379")
    .getOrCreate()

  import spark.implicits._

  val sc:SparkContext = spark.sparkContext
  val ssc:StreamingContext = new StreamingContext(sc, Seconds(BatchDuration))

  val sentimentAnnotator:PipelineModel = PretrainedPipeline("analyze_sentiment", lang= "en").model // Only english model available

  val documentAssembler:DocumentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  val tokenizer:Tokenizer = new Tokenizer()
    .setInputCols("document")
    .setOutputCol("token")

  // Stopwords model : ! language hardcoded !
  val stopWordsCleaner:StopWordsCleaner = StopWordsCleaner.pretrained("stopwords_fr", lang = "fr")
    .setInputCols("token")
    .setOutputCol("clean_text")

  val finisher:Finisher = new Finisher()
    .setInputCols("clean_text", "sentiment")

  val cleaningPipeline:Pipeline = new Pipeline().setStages(Array(
    documentAssembler,
    tokenizer,
    stopWordsCleaner,
    sentimentAnnotator,
    finisher
  ))

  // Redis TTL config
  val raw_stream_ttl:Int = 0
  val clean_stream_ttl:Int = 0
  val wordcount_ttl:Int = 0

  // WordCount config
  val limit:Int = 10000

  def start(): Unit = {

    sys.ShutdownHookThread {
      //Executes when shutdown signal is received by the app
      println("Gracefully stopping Spark Context")
      sc.stop()
      ssc.stop(true, true)
      println("Application stopped")
    }

    // Init CustomReceiver from IRCBot
    val customReceiverStream = ssc.receiverStream(new IRCBot(TwitchChannel, twitch_nick, twitch_oauth))

    // Init temporary view for Redis
    spark.sql(
      """
        |CREATE TEMPORARY VIEW wordcount (words STRING, counts INT)
        | USING org.apache.spark.sql.redis OPTIONS (table 'wordcount', key.column 'words')
        |""".stripMargin
    )

    // Run Streaming Operations
    customReceiverStream foreachRDD { rdd =>

      // Preprocess raw twitch IRC stream
      val df = rdd.toDF()
          .withColumnRenamed("value", "message")
          .withColumn("_tmp", split($"message", ":"))
          .select(
            $"message",
            $"_tmp".getItem(0).alias("timestamp"),
            $"_tmp".getItem(1).alias("metadata"),
            $"_tmp".getItem(2).alias("text"),
          )
          .withColumn("_tmp", split($"metadata", " "))
          .select(
            $"message",
            $"metadata",
            $"timestamp",
            split($"_tmp".getItem(0), "!").getItem(0).alias("user"),
            $"_tmp".getItem(2).alias("channel"),
            lower($"text").alias("text")
          )

      df.createOrReplaceTempView("words")
      write(df, table = s"$TwitchChannel" + "_raw", ttl = raw_stream_ttl , mode = SaveMode.Append)

      println("Cleaning data using SparkNLP pipeline...")

      // Fit-Transform using SparkNLP pipeline
      val clean_df = cleaningPipeline.fit(df).transform(df)

      // If any results, process
      if (clean_df.count() > 0) {

        println("Writing clean stream...")

        // Write cleaned stream to Redis
        write(clean_df, table = s"$TwitchChannel" + "_cleaned_stream", ttl = clean_stream_ttl , mode = SaveMode.Append)

        // Get sentiment counts and update from previous results in Redis
        println("Updating sentiment count...")
        val sentiment_counts = clean_df.transform(sentimentCount)
        val sentiment_counts_schema = StructType(Array(
          StructField("finished_sentiment", StringType),
          StructField("sentiment_count", IntegerType)
        ))
        val prev_sentiment_counts = get_table(
          s"$TwitchChannel" + "_sentiment_counts",
          sentiment_counts_schema,
          "finished_sentiment"
        )
        val new_sentiment_counts = update_table(
          prev_sentiment_counts,
          sentiment_counts,
          "finished_sentiment",
          "sentiment_count"
        )
        write(
          new_sentiment_counts,
          table = s"$TwitchChannel" + "_sentiment_counts",
          ttl = wordcount_ttl ,
          mode = SaveMode.Overwrite,
          keyColumn = "finished_sentiment"
        )

        // Get current wordcount and update previous results in Redis
        print("Updating wordcount...")
        val wordcount = clean_df.transform(getWordcount)
        wordcount.createOrReplaceTempView("wordcount")
        val wordcount_schema = StructType(Array(
          StructField("words", StringType),
          StructField("count", FloatType)
        ))
        val prev_wordcount = get_table(
          s"$TwitchChannel" + "_wordcount",
          wordcount_schema,
          "words"
        )
        val summed_counts = update_table(
          prev_wordcount,
          wordcount,
          "words",
          "count"
        )
        write(
          summed_counts,
          table = s"$TwitchChannel" + "_wordcount",
          ttl = wordcount_ttl ,
          mode = SaveMode.Append,
          keyColumn = "words"
        )

      }
      else {
        println("No new message to process !")
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def sentimentCount(df: DataFrame): DataFrame = {
    /* Notes
    - Explode not averaging message.
     */
    df
      .select(explode($"finished_sentiment").alias("finished_sentiment"))
      .groupBy("finished_sentiment")
      .agg(count("finished_sentiment").alias("sentiment_count"))
      .na.fill("None")
  }

  def getWordcount(df:DataFrame): DataFrame = {
    df
      .select(explode(col("finished_clean_text")).alias("words"))
      .select("words")
      .filter(length($"words") > 2)
      .groupBy($"words")
      .count()
      .sort($"count".desc)
      .limit(10000)
  }

  def write(df: DataFrame, table: String, ttl: Int = 300,
             mode: SaveMode = SaveMode.Append, keyColumn:String = null
           ): Unit = {

    if (keyColumn != null) {
      df
        .write
        .format("org.apache.spark.sql.redis")
        .option("table", table)
        .option("key.column", keyColumn)
        .option("ttl", ttl)
        .mode(mode)
        .save()
    } else {
      df
        .write
        .format("org.apache.spark.sql.redis")
        .option("table", table)
        .option("ttl", ttl)
        .mode(mode)
        .save()
    }

  }

  def write_to_file(df:DataFrame): Unit = {
    df
      .coalesce(1)
      .write
      .format("csv")
      .mode(saveMode = SaveMode.Append)
      .save("/home/neadex/scala/twitch_streaming_extract.csv")
  }

  def get_table(tableName:String, schema:StructType, keyColumn:String = null): DataFrame = {

    val df = if (keyColumn != null) {
      scala.util.Try({
        spark.read
          .format("org.apache.spark.sql.redis")
          .option("table", tableName)
          .option("key.column", keyColumn)
          .schema(schema)
          .load()
      }).getOrElse({
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      })
    } else {
      scala.util.Try({
        spark.read
          .format("org.apache.spark.sql.redis")
          .option("table", tableName)
          .schema(schema)
          .load()
      }).getOrElse({
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      })
    }
    return df

  }

  def update_table(df1:DataFrame, df2:DataFrame, joinKey:String, sumKey:String): DataFrame = {

    val new_df = df1
        .join(
          df2.withColumnRenamed(sumKey, "t+1"), Seq(joinKey),
          "full_outer"
        ).na.fill(0)
      .withColumnRenamed(sumKey, "old_sumkey")
      .withColumn(sumKey, (col("old_sumkey") + col("t+1")))
      .drop("old_sumkey", "t+1")

    return new_df
  }

}
