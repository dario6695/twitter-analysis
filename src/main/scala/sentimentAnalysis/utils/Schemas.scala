package sentimentAnalysis.utils

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Schemas {

  val tweeterStatusSchema = StructType(Array(
    StructField("id", LongType),
    StructField("text", StringType),
    StructField("timestamp_ms", StringType)
  ))

}
