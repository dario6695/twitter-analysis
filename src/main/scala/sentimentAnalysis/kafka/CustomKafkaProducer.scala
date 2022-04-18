package sentimentAnalysis.kafka

trait CustomKafkaProducer {

  def produce(value: String): Unit

}
