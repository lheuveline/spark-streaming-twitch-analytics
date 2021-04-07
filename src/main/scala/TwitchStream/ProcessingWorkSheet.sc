import com.johnsnowlabs.nlp.annotators._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher, SparkNLP}
import org.apache.spark._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val TwitchChannel:String = "#alphacast"
val dataDir:String = "/home/neadex/scala/twitch_streaming_extract.csv"


val schema = StructType(Array(
  StructField("message", StringType),
  StructField("metadata", StringType),
  StructField("timestamp", StringType),
  StructField("user", StringType),
  StructField("channel", StringType),
  StructField("text", StringType)
))

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


val sentimentAnnotator:PipelineModel = PretrainedPipeline("analyze_sentiment", lang = "en").model // Only english model available

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

// START PROCESSING
val df = spark
  .read
  .format("csv")
  .schema(schema)
  .load(dataDir)
  .limit(50) // Java Heap Space ?

val clean_df = cleaningPipeline.fit(df).transform(df)

def sentimentCount(df: DataFrame): DataFrame = {
  /* Notes
  - Explode not averaging message.
  - Need to add update mechanism like wordcount update
   */
  df
    .select(explode($"finished_sentiment").alias("finished_sentiment"))
    .groupBy("finished_sentiment")
    .agg(count("finished_sentiment").alias("sentiment_count"))
}

val sentiment_counts = clean_df.transform(sentimentCount)

def update_table(
                  df1:DataFrame,
                  df2:DataFrame,
                  joinKey:String,
                  sumKey:String
                ): DataFrame = {

  val new_df = df1.union(df2)
    .groupBy(joinKey)
    .agg(sum(sumKey).alias(sumKey))

  return new_df
}

// Create test dataframes
val df1 = sentiment_counts

// Create empty DataFrame to emulate empty previous count
val schema = StructType(Array(
  StructField("finished_sentiment", StringType, true),
  StructField("sentiment_counts", IntegerType, true)
))
val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)


// Inverting df1 and df2 to emulate empty current dataframe
val summed_df = update_table(
  df2,
  df1,
  "finished_sentiment",
  "sentiment_counts"
)

df1.show()
df2.show()
summed_df.show()
