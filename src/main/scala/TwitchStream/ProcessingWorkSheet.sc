import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable

val dataDir:String = "/home/neadex/TwitchStream/ml/data/frwiki_discussions_categories.csv"

val schema = StructType(Array(
  StructField("title", StringType),
  StructField("text", StringType),
  StructField("discussions", StringType),
  StructField("categories", StringType)
))

// Init SparkNLP Pipeline
val spark = SparkSession.builder()
  .appName(s"TwitchStream_train")
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

// Load data
val df = spark
  .read
  .format("csv")
  .option("wholeFile", "true")
  .option("multiline",true)
  .option("header", true)
  .option("escape","\"")
  .schema(schema)
  .load(dataDir)

val clean_disc = udf[String, String](str => {
  str
    .replaceAll("< br / >", "")
    .replaceAll("\n", "")
})

// Manually found outliers, used in filtering
// Too frequent (ex: "Sélection transversale")
// Useless (ex "avancement=B  ")
// TO-DO : rtrim seems not working, see below duplicates with trailing whitespace
val outliers = Array(
  "Sélection transversale",
  "Sélection francophone",
  "avancement=B",
  "avancement=B  ",
  "avancement=AdQ",
  "avancement=BA",
  "WP1.0=oui",
  "WP1.0=oui  ",
  "avancement=A",
  "élevée",
  "élevée  ",
  "maximum  ",
  "maximum"
)

val cast_categories = udf[String, String](str => {
  val toRemove = """[]"'""".toSet
  str.filterNot(toRemove)
})

val clean_df = df
  .na.drop()
  .select(
    $"discussions".alias("text"),
    $"categories".alias("label")
  )
  .select( // transformation steps
    split(rtrim(ltrim(clean_disc($"text"))), ",").alias("text"),
    split(rtrim(ltrim(cast_categories($"label"))), ", ").alias("label")
  )
  .select( // Explode discussion arrays. Text
    explode($"text").alias("text"), $"label"
  )
  .select( // Clean resulting str
    rtrim(ltrim($"text")).alias("text"),
    $"label",
  )
  .withColumn("outliers", typedLit(outliers))
  .filter(
    length($"text") > 50
  )
  .select( // Remove class outliers
    $"text",
    array_except($"label", $"outliers").alias("label")
  )

val maxLabels = 100 // Number of labels to keep in training dataset
val top_labels = clean_df
  .select(
    $"text",
    explode($"label").alias("label")
  )
  .groupBy("label")
  .count()
  .sort($"count".desc)
  .select($"label")
  .limit(maxLabels)
  .as[String] // Convert to dataset for type cast
  .collect()

// Remove all labels not contained in
def removeLabels(toKeep:Seq[String]): UserDefinedFunction = {
  udf((c: mutable.WrappedArray[String]) =>
    c intersect toKeep)
}
val final_df = clean_df
  .select(
    $"text",
    removeLabels(top_labels)($"label").alias("label")
  )
  .filter(size($"label") > 0)

val outputDir:String = "/home/neadex/TwitchStream/ml/data/frwiki_discussions_categories"

final_df
  .coalesce(1)
  .write
  .mode(SaveMode.Overwrite)
  .parquet(outputDir)
