package sentimentAnalysis.kafka

import java.util.Properties

object KafkaConfig {

  lazy val tweetKafkaProps: Properties = getConfig


  private def getConfig: Properties = {
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props
  }

}
