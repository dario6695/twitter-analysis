package sentimentAnalysis

import sentimentAnalysis.utils.Schemas.tweeterStatusSchema
import sentimentAnalysis.entities.TweeterStatus
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import sentimentAnalysis.kafka.{CustomKafkaProducerImpl, KafkaConfig}
import sentimentAnalysis.twitter4j.{Tweeter4jConfigurations, TweeterStreamStarter}
import sentimentAnalysis.utils.Constants._
import sentimentAnalysis.utils.PostgresConf


object TweeterProject {

  Logger.getLogger("org").setLevel(Level.WARN)


  val spark = SparkSession.builder()
    .appName("Twitter analysis")
    .master("local[*]")
    .getOrCreate()


  import spark.implicits._

  def readTweeterStatusStreaming(): Dataset[TweeterStatus] = {

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "tweeter_data")
      .load()
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .select(from_json(col("actualValue"), tweeterStatusSchema).as("tweeterStatus"))
      .selectExpr("tweeterStatus.*")
      .as[TweeterStatus]

  }


  def persistTweetsStreaming() = {

    val twitterDS: Dataset[TweeterStatus] = readTweeterStatusStreaming()

    twitterDS.writeStream
      .foreachBatch { (batch: Dataset[TweeterStatus], _: Long) =>
        batch.write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", PostgresConf.DRIVER)
          .option("url", PostgresConf.URL)
          .option("user", PostgresConf.USER)
          .option("password", PostgresConf.PASSWORD)
          .option("dbtable", "public.tweeterstatus")
          .save()
      }
      .start()
      .awaitTermination()

  }

  //docker exec -it postgresContainer psql -U docker -d tweeter

  /**

  \list or \l: list all databases
  \c <db name>: connect to a certain database
  \dt: list all tables in the current database using your search_path
  \dt *.: list all tables in the current database regardless your search_path

    */

  def main(args: Array[String]): Unit = {

    val tweeterKafkaProducer = new CustomKafkaProducerImpl(TWEET_KAFKA_TOPIC, KafkaConfig.tweetKafkaProps)

    val twitterStreamStarter = new TweeterStreamStarter(tweeterKafkaProducer)

    twitterStreamStarter.getTwitterStream()

    persistTweetsStreaming()

  }


}
