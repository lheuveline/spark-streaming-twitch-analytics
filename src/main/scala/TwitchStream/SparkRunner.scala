package TwitchStream

import com.johnsnowlabs.nlp.annotators._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher, SparkNLP}
import org.apache.spark._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql._
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
    .appName("TwitchStream")
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
  val raw_stream_ttl:Int = 30
  val clean_stram_ttl:Int = 30
  val wordcount_ttl:Int = 300


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

      println("SparkStreaming foreach iteration start")

      // Preprocess raw twitch IRC stream
      /*
      val df = rdd
        .toDF()
        .withColumnRenamed("value", "message")
        .withColumn("_tmp", split($"message", ":")) // split only once if msg contains ":"
        .select(
          $"message",
          $"_tmp".getItem(1).alias("metadata"),
          $"_tmp".getItem(2).alias("text")
        )
        .withColumn("_tmp", split($"metadata", " "))
        .select(
          $"message",
          $"_tmp".getItem(0).alias("timestamp"),
          $"_tmp".getItem(1).alias("full_user"),
          $"_tmp".getItem(2).alias("channel"),
          $"text"
        )
        .withColumn("user", split($"full_user", "!").getItem(0))

       */

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
      write(df, table = "raw", ttl = raw_stream_ttl , mode = SaveMode.Append)

      // Fit-Transform using SParkNLP pipeline
      val clean_df = cleaningPipeline.fit(df).transform(df)
      // write(clean_df, table = "cleaned_stream", ttl = clean_stream_ttl , mode = SaveMode.Append)

      if (clean_df.count() > 0) {

        // Get current wordcount
        val wordcount = clean_df
          .select(explode($"finished_clean_text").alias("words"))
          .select("words")
          .filter(length($"words") > 2)
          .groupBy($"words")
          .count()
          .sort($"count".desc)
          .limit(10)
        wordcount.createOrReplaceTempView("wordcount")

        // Get previous wordcount from Redis - Could be kept in memory instead of make new request
        val prev_wordcount = scala.util.Try({
          spark.read
            .format("org.apache.spark.sql.redis")
            .option("table", "wordcount")
            .option("key.column", "words")
            .schema(StructType(Array(
              StructField("words", StringType),
              StructField("count", FloatType)
            )))
            .load()
        }).getOrElse({
          spark.emptyDataFrame
        })
        prev_wordcount.createOrReplaceTempView("prev_wordcount")

        val new_wordcount = spark.sql(
          """
            |SELECT
            | wordcount.words AS wordcount_words,
            | wordcount.count AS wordcount_count,
            | prev_wordcount.words AS prev_wordcount_words,
            | prev_wordcount.count AS prev_wordcount_count
            |FROM wordcount
            |FULL OUTER JOIN prev_wordcount
            |ON wordcount.words = prev_wordcount.words
            |""".stripMargin).na.fill(0)
            .withColumn(
              "all_words",
              when($"wordcount_words".isNull, $"prev_wordcount_words").otherwise($"wordcount_words")
            )
            .select($"all_words".alias("words"), $"wordcount_count", $"prev_wordcount_count")

        val sum_counts: (Int, Int) => Int = (x:Int, y:Int) => x + y
        val sum_udf = udf(sum_counts)

        val summed_counts = new_wordcount
            .withColumn("new_sum", sum_udf($"wordcount_count", $"prev_wordcount_count"))
            .select(
              $"words",
              $"wordcount_count",
              $"prev_wordcount_count",
              $"new_sum".alias("count")
            )
        // summed_counts.sort($"count".desc).show()

        // Write new words to Redis, using TTL to clean old words
        summed_counts
          .write
          .format("org.apache.spark.sql.redis")
          .option("table", "wordcount")
          .option("ttl", wordcount_ttl)
          .option("key.column", "words")
          .option("scan.count", "1000")
          .option("stream.parallelism", 4)
          .mode(SaveMode.Append)
          .save()

        println("SparkStreaming foreach iteration end")

      }
      else {
        println("No new message to process !")
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def sentimentCount(df: DataFrame): DataFrame = {
    df
      .withColumn("sentiment", when(rand() > 0.5, 1).otherwise(0))
      .groupBy("sentiment")
      .agg(count("sentiment").alias("sentiment_count"))
  }

  def write(df: DataFrame, table: String, ttl: Int = 300, mode: SaveMode = SaveMode.Append): Unit = {
    df
      .write
      .format("org.apache.spark.sql.redis")
      .option("table", table)
      .option("ttl", ttl)
      .mode(mode)
      .save()
  }

}
