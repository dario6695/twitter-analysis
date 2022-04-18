package sentimentAnalysis.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import sentimentAnalysis.utils.Constants.TWEET_KAFKA_TOPIC

import java.util.Properties

class CustomKafkaProducerImpl(topic: String, properties: Properties) extends CustomKafkaProducer {


  override def produce(value: String): Unit = {
    try {
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](TWEET_KAFKA_TOPIC, value)

      producer.send(record);
      producer.close();
    } catch {
      case ex: Throwable => throw new RuntimeException(s"Error during the production of events in kafka: ${ex.getMessage}")
    }
  }

}
