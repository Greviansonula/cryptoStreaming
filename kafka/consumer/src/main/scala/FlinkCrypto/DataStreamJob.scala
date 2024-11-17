package FlinkCrypto

import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import utils.{DataParser, DataReducer}

object DataStreamJob {
  def main(args: Array[String]): Unit = {
    // Sets up the execution environment, which is the main entry point
    // to building Flink applications.
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSourceTopic = "crypto_ex_rates"
    val kafkaBootstrapServers = "localhost:9092"

    // Kafka Source
    val kafkaSource = KafkaSource.builder[String]
      .setBootstrapServers(kafkaBootstrapServers)
      .setTopics(kafkaSourceTopic)
      .setGroupId("crypto-stream-group")
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    // Parse, transform, and reduce data
    val parsedStream = stream
      .map(DataParser.parse _)
      .keyBy(_.currency)
      .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.minutes(1))
      .reduce(DataReducer.reduce _)

    // Sink: Print to console (or send to other sinks like a database)
    parsedStream.print()

    // Execute the Flink job
    env.execute("Flink Scala API Skeleton")
  }
}