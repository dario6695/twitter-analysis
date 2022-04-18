package sentimentAnalysis.twitter4j

import sentimentAnalysis.kafka.CustomKafkaProducer
import twitter4j._

import java.io._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TwitterStreamStarter(tweetKafkaProducer: CustomKafkaProducer) {


  private def statusListener: StatusListener = new StatusListener {

    override def onStatus(status: Status): Unit = {

      val json: String = TwitterObjectFactory.getRawJSON(status)

      tweetKafkaProducer.produce(json)

    }

    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()

    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()

    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()

    override def onStallWarning(warning: StallWarning): Unit = ()

    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }


  def getTwitterStream(): Future[TwitterStream] = Future {
    try {
      new TwitterStreamFactory(Twitter4jConfigurations.configBuilder.build())
        .getInstance()
        .addListener(statusListener)
        .sample("en")

    } catch {
      case ex: Throwable => throw new IOException(s"Error during IO operation: ${ex.getMessage}")
    }
  }
}
