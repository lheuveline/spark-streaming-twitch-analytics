package TwitchStream

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object transform {

  def getCategory(df:DataFrame): DataFrame = {

    /*
    Annotate DataFrame using trained model on through TensorFlow Serving Calls
    Input: String - raw text to annotate
    Ouput : List[String] - list of detected categories

    Model : transfer learning using UniversalSentenceEncoder and revision discussions from Wikimedia
    [link_to_repository]
     */

    val categoryUdf = udf(x => functions.annotateText(x))
    df
      .withColumn("category", categoryUdf(col("text")))
  }

  def sentimentCount(df: DataFrame): DataFrame = {
    /* Notes

    - Explode not averaging message.

     */
    df
      .select(explode(col("finished_sentiment")).alias("finished_sentiment"))
      .groupBy("finished_sentiment")
      .agg(count("finished_sentiment").alias("sentiment_count"))
      .na.fill("None")
  }

  def getWordcount(df:DataFrame): DataFrame = {
    df
      .select(explode(col("finished_clean_text")).alias("words"))
      .select("words")
      .filter(length(col("words")) > 2)
      .groupBy(col("words"))
      .count()
      .sort(col("count").desc)
      .limit(10000)
  }
}
