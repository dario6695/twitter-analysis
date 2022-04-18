package sentimentAnalysis.spark.schema

import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}

object Schemas {

  val twitterStatusSchema: StructType = StructType(Array(
    StructField("id", LongType),
    StructField("text", StringType),
    StructField("timestamp_ms", StringType)
  ))


  val twitterDeepInfoSchema: StructType = StructType(Array(
    StructField("id", LongType),
    StructField("text", StringType),
    StructField("timestamp_ms", StringType),
    StructField("user", StructType(Array(
      StructField("id", LongType),
      StructField("location", StringType)
    ))),
    StructField("entities", StructType(Array(
      StructField("hashtags", ArrayType(StringType))
    )))
  ))


  val twitterDeepInfoRestDtoSchema: StructType = StructType(Array(
    StructField("id", LongType),
    StructField("text", StringType),
    StructField("timestamp_ms", StringType),
    StructField("userId", LongType),
    StructField("userLocation", StringType),
    StructField("hashtags", StringType)
  ))

}
