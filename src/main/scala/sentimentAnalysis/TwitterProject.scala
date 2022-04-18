package sentimentAnalysis

import org.apache.log4j.{Level, Logger}
import sentimentAnalysis.dataModel.TwitterDeepInfoMapper
import sentimentAnalysis.kafka.{CustomKafkaProducerImpl, KafkaConfig}
import sentimentAnalysis.spark.SparkJobExecutorService
import sentimentAnalysis.twitter4j.TwitterStreamStarter
import sentimentAnalysis.utils.Constants._


object TwitterProject {

  Logger.getLogger("org").setLevel(Level.WARN)


  def main(args: Array[String]): Unit = {

    val twitterKafkaProducer = new CustomKafkaProducerImpl(TWEET_KAFKA_TOPIC, KafkaConfig.tweetKafkaProps)

    val twitterStreamStarter = new TwitterStreamStarter(twitterKafkaProducer)

    twitterStreamStarter.getTwitterStream()

    val sparkJobExecutor = new SparkJobExecutorService(TwitterDeepInfoMapper.toDto)

    sparkJobExecutor.persistDeepTweetsStreaming()

    //sparkJobExecutor.persistTweetsStreaming()

    //sparkJobExecutor.persistTweetsBatch()

  }


}
