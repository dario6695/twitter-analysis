package sentimentAnalysis.spark

import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import sentimentAnalysis.dataModel.entities.{TwitterDeepInfo, TwitterStatus}
import sentimentAnalysis.dataModel.repositoryDto.TwitterDeepInfoRepoDto
import sentimentAnalysis.spark.schema.Schemas.{twitterDeepInfoSchema, twitterStatusSchema}
import sentimentAnalysis.utils.PostgresConf
import zio.{Task, UIO, ZIO, ZLayer}


trait SparkJobExecutorService {

  def persistTweetsStreaming(): Task[Unit]

  def persistTweetsBatch(): Task[Unit]

  def persistDeepTweetsStreaming(): Task[Unit]

}


object SparkJobExecutorServiceLive {
  def layer(twitterDeepInfoToDto: TwitterDeepInfo => TwitterDeepInfoRepoDto): ZLayer[Any, Throwable, SparkJobExecutorServiceLive] =
    ZLayer {
      ZIO.attempt(SparkJobExecutorServiceLive(twitterDeepInfoToDto))
    }
}


object SparkJobExecutorService {
  def persistTweetsStreaming() = ZIO.serviceWith[SparkJobExecutorService](_.persistTweetsStreaming)

  def persistTweetsBatch() = ZIO.serviceWith[SparkJobExecutorService](_.persistTweetsBatch)

  def persistDeepTweetsStreaming() = ZIO.serviceWith[SparkJobExecutorService](_.persistDeepTweetsStreaming)
}

case class SparkJobExecutorServiceLive(
                                        twitterDeepInfoToDto: TwitterDeepInfo => TwitterDeepInfoRepoDto
                                      ) extends SparkJobExecutorService {

  val spark: SparkSession = SparkSession.builder()
    .appName("Twitter analysis")
    .master("local[*]")
    .getOrCreate()


  import spark.implicits._

  private def readTweeterStatusStreaming(): Task[Dataset[TwitterStatus]] = {
    ZIO.scoped {
      ZIO.attempt {
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
    }
  }


  //tweet data inside postgres
  def persistTweetsStreaming(): Task[Unit] =
    ZIO.scoped {
      for {
        twitterDS <- readTweeterStatusStreaming()
        twitterDsFiltered <- filterCustomTwitterStatus(twitterDS)
        _ <- ZIO.attempt {
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
      } yield ()
    }


  private def readTwitterStatusBatch(): Task[Dataset[TwitterStatus]] =
    ZIO.scoped {
      ZIO.attempt {
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
    }


  def persistTweetsBatch(): Task[Unit] = {
    ZIO.scoped {
      for {
        twitterDS <- readTwitterStatusBatch()
        twitterDsFiltered <- filterCustomTwitterStatus(twitterDS)
        _ <- ZIO.attempt {
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
      } yield ()
    }
  }


  private def readDeepTweetsStatusStreaming(): Task[Dataset[TwitterDeepInfo]] =
    ZIO.scoped {
      ZIO.attempt {
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
    }

  def persistDeepTweetsStreaming(): Task[Unit] =
    ZIO.scoped {
      for {
        twitterDS <- readDeepTweetsStatusStreaming()
        _ <- ZIO.succeed(twitterDS.printSchema())
        filteredDS <- operationsOverDeepTweets(twitterDS)
        _ <- ZIO.succeed(filteredDS.printSchema())
        _ <- ZIO.attempt {
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
      } yield ()
    }


  def operationsOverDeepTweets(twitterDs: Dataset[TwitterDeepInfo]): UIO[Dataset[TwitterDeepInfoRepoDto]] =
    ZIO.succeed {
      twitterDs
        .map(twitterDeepInfoToDto)
        .filter(tweet => tweet.text.toLowerCase.contains("twitter") & tweet.hashtags.contains("elonmusk"))
    }


  def filterCustomTwitterStatus(twitterDs: Dataset[TwitterStatus]): UIO[Dataset[TwitterStatus]] =
    ZIO.succeed {
      twitterDs
        .filter(tweet => tweet.text.toLowerCase.contains("twitter"))
    }


}
