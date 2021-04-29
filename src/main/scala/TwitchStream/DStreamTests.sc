import TwitchStream._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

import scala.collection.mutable

// TF Serving imports

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.json4s.jackson.{JsonMethods, Serialization}

/*

General function for annotating texts:
  - Call to TF Serving
  - Label using python fitted MultiLabelEncoder using embeded json
  - Filter predictions from model using difficulty threshold ( default 0.5 )

 */

def make_tensorflow_serving_call(text:String): Map[String, List[Double]] = {

  // Make HTTP Call to TensorFlow Serving.
  // Need static IP for TFServing

  val containerIp = "172.25.0.2"
  val tfsPort = "8501"
  val api_endpoint = f"http://$containerIp:$tfsPort/v1/models/frwikimedia_use_50_cat:predict"
  val httpClient = HttpClients.createDefault()

  val postRequest = new HttpPost(api_endpoint)

  val data: Map[String, Array[String]] = Map(
    "instances" -> Array(text)
  )

  implicit val formats = org.json4s.DefaultFormats
  val jsonData = Serialization.write(data)
  val postData = new StringEntity(jsonData)
  postRequest.setEntity(postData)
  val resp = httpClient.execute(postRequest)
  val result = EntityUtils.toString(resp.getEntity)

  JsonMethods.parse(result).values.asInstanceOf[Map[String, List[Double]]]
}

def load_classes_labels(): Map[String, String] = {
  val src = scala.io.Source.fromResource("encoder_classes.json")
  JsonMethods.parse(src.reader()).values.asInstanceOf[Map[String, String]]
}

def formatPredictions(preds:List[Int], labels:Map[String, String]): List[Option[String]] = {

  /*

  Take list of filtered predictions and get labelName from index

   */

  // Values inverse sort
  val selectedLabels = preds.map(k => labels.get(k.toString))

  selectedLabels
}

def filterPredictions(preds:List[Double], threshold: Double): List[Int] = {
  val filteredPreds = preds.filter(x => x > threshold).map(x => preds.indexOf(x))
  filteredPreds
}

def getLabels(preds:List[Double]): List[Option[String]] = {

  // Get classes labels
  val labels = load_classes_labels()

  // filterPred
  val filteredPreds = filterPredictions(preds, threshold =  0.5)

  // format filtered preds
  val formattedPredictions = formatPredictions(filteredPreds, labels)
  formattedPredictions
}

// Annotate text using TF Serving
def annotateText(text:String): List[Option[String]] = {
  /*

    Wrapper to annotate text using TFServing and decode labels

    TO-DO : move encoder classes to Redis at train / validation time

   */

  val tfs_response = make_tensorflow_serving_call(text)
  val preds = tfs_response("predictions").asInstanceOf[List[List[Double]]].flatten // Not working with multiple texts
  val filteredPreds = getLabels(preds)
  filteredPreds
}


//   Write DataFrame to Redis
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

val TwitchChannel = "#antoinedaniellive"
val BatchDuration = 1
val twitch_nick = "throwable_dummy"
val twitch_oauth = "oauth:0ie5ahxufz4zyski1etgl8gckdurvu"

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

// For stream states
ssc.checkpoint(".")

sys.ShutdownHookThread {
  //Executes when shutdown signal is received by the app
  println("Gracefully stopping Spark Context")
  sc.stop()
  ssc.stop(true, true)
  println("Application stopped")
}

val customReceiverStream = ssc.receiverStream(new IRCBot(TwitchChannel, twitch_nick, twitch_oauth))

// Format raw message stream from Twitch IRC
def formatRawDataFrame(df:DataFrame): DataFrame = {
  df.withColumnRenamed("value", "message")
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
}


// Running count by key.
def updateFunction(newValues: Seq[(Int)], runningCount: Option[(Int)]): Option[(Int)] = {

  var result: Option[(Int)] = null

  if ( newValues.isEmpty ) { //check if the key is present in new batch if not then return the old values
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
}

// Need sqlContext implicits for reduceByKey usage
import spark.sqlContext.implicits._

// Entry stream : clean + stopwords + tokenize using spark NLP pipeline
val mainStream = customReceiverStream.transform(rdd => {

  val cleaningPipeline = pipelines.getGenericPipeline()
  val df = rdd.toDF()
  val formattedDF = df.transform(formatRawDataFrame)
  val cleanDF = cleaningPipeline.fit(formattedDF).transform(formattedDF)

  cleanDF.rdd
})

// CategoryCount
val categoryStream = mainStream.transform(rdd => {

  /* DStream to extract categories from text using TF serving */

  val categoryCountTtl = 0

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
  val categoryUdf = udf(x => annotateText(x))

  // Return category column as RDD
  df
    .withColumn("category", categoryUdf(col("raw")))
    .select(explode($"category").alias("category"))
    .groupBy("category")
    .count()
    .filter($"count" > 1)
    .rdd
})

// Update CategoryCount
val categoryCount = categoryStream
  .map{x => (x.getAs[String](0), x.getAs[Int](1))}
  .reduceByKey((x, y) => x + y)
  .updateStateByKey[Int](updateFunction _)

// WordCount and update state
val wordCountStream = mainStream.map(row => {
  val pairs = row.getAs[mutable.WrappedArray[String]](6).toArray.map(x => (x, 1))
  pairs
}).flatMap(x => x)
  .filter(x => x._1.length() > 3) // Filter too short words. Should be removed from mainStream in finished_clean_text
  .reduceByKey((x, y) => x + y)
  .updateStateByKey[Int](updateFunction _)

// Write WordCount to Redis
wordCountStream.foreachRDD(rdd => {
  val wordCountTtl = 0
  val df = spark.createDataFrame(rdd)
  write(df, table = s"$TwitchChannel" + "_wordcount",
    ttl = wordCountTtl , mode = SaveMode.Append)
})

// Write categoryCount to Redis
categoryCount.foreachRDD(rdd => {
  val categoryCountTtl = 0
  val df = spark.createDataFrame(rdd)
  write(df, table = s"$TwitchChannel" + "_categoryCount",
    ttl = categoryCountTtl , mode = SaveMode.Append)
})

ssc.start()
ssc.awaitTermination()
