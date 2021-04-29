package TwitchStream

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class RedisInterface(spark:SparkSession) {

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
