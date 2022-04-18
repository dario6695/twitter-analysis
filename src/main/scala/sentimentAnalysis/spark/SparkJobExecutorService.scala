package sentimentAnalysis.spark

import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import sentimentAnalysis.dataModel.entities.{TwitterDeepInfo, TwitterStatus}
import sentimentAnalysis.dataModel.repositoryDto.TwitterDeepInfoRepoDto
import sentimentAnalysis.spark.schema.Schemas.{twitterDeepInfoSchema, twitterStatusSchema}
import sentimentAnalysis.utils.PostgresConf

class SparkJobExecutorService(
                             twitterDeepInfoToDto: TwitterDeepInfo => TwitterDeepInfoRepoDto
                             ) {

  val spark = SparkSession.builder()
    .appName("Twitter analysis")
    .master("local[*]")
    .getOrCreate()


  import spark.implicits._

  private def readTweeterStatusStreaming(): Dataset[TwitterStatus] = {

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitter_data")
      .load()
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .select(from_json(col("actualValue"), twitterStatusSchema).as("twitterStatus"))
      .selectExpr("twitterStatus.*")
      .as[TwitterStatus]

  }


  //persist Ikea related tweet data inside postgres
  def persistTweetsStreaming() =  {

    val twitterDS: Dataset[TwitterStatus] = readTweeterStatusStreaming()

    val twitterDsFiltered = filterIkeaTwitterStatus(twitterDS)


    twitterDsFiltered.writeStream
      .foreachBatch { (batch: Dataset[TwitterStatus], _: Long) =>
        batch.write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", PostgresConf.DRIVER)
          .option("url", PostgresConf.URL)
          .option("user", PostgresConf.USER)
          .option("password", PostgresConf.PASSWORD)
          .option("dbtable", "public.twitterstatus")
          .save()
      }
      .start()
      .awaitTermination()

  }




  private def readTwitterStatusBatch(): Dataset[TwitterStatus] = {

    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitter_data")
      .load()
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .select(from_json(col("actualValue"), twitterStatusSchema).as("twitterStatus"))
      .selectExpr("twitterStatus.*")
      .as[TwitterStatus]

  }
  def persistTweetsBatch() = {

    val twitterDS: Dataset[TwitterStatus] = readTwitterStatusBatch()

    val twitterDsFiltered = filterIkeaTwitterStatus(twitterDS)


    twitterDsFiltered.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", PostgresConf.DRIVER)
      .option("url", PostgresConf.URL)
      .option("user", PostgresConf.USER)
      .option("password", PostgresConf.PASSWORD)
      .option("dbtable", "public.twitterstatusBatch")
      .save()


  }



  private def readDeepTweetsStatusStreaming(): Dataset[TwitterDeepInfo] = {

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitter_data")
      .load()
      .select(col("topic"), expr("cast(value as string) as actualValue"))
      .select(from_json(col("actualValue"), twitterDeepInfoSchema).as("twitterStatus"))
      .selectExpr("twitterStatus.*")
      .as[TwitterDeepInfo]

  }

  def persistDeepTweetsStreaming() =  {
    val twitterDS: Dataset[TwitterDeepInfo] = readDeepTweetsStatusStreaming()

    twitterDS.printSchema()

    val filteredDS = operationsOverDeepTweets(twitterDS)

    filteredDS.printSchema()

    filteredDS.writeStream
      .foreachBatch { (batch: Dataset[TwitterDeepInfoRepoDto], _: Long) =>
        batch.write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", PostgresConf.DRIVER)
          .option("url", PostgresConf.URL)
          .option("user", PostgresConf.USER)
          .option("password", PostgresConf.PASSWORD)
          .option("dbtable", "public.twitterdeepstatus")
          .save()
      }
      .start()
      .awaitTermination()
  }

  def operationsOverDeepTweets(twitterDs: Dataset[TwitterDeepInfo]): Dataset[TwitterDeepInfoRepoDto] = {

    twitterDs
      .map(twitterDeepInfoToDto)
      .filter(tweet => tweet.text.toLowerCase.contains("ikea") & tweet.hashtags.contains("sustainability"))
  }



  def filterIkeaTwitterStatus(twitterDs: Dataset[TwitterStatus]): Dataset[TwitterStatus] = {
    twitterDs
      .filter(tweet => tweet.text.toLowerCase.contains("ikea"))
  }


}
