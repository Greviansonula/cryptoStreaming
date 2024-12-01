package FlinkCrypto

import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import utils.{DataParser, DataReducer}
import Deserializer.CryptoDataDeserializer
import Dto.CryptoData
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder,ElasticsearchEmitter, RequestIndexer}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType
import java.util.HashMap
import org.apache.flink.api.connector.sink2.SinkWriter
import org.slf4j.LoggerFactory
import java.util.{HashMap, UUID}
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import scala.collection.JavaConverters._
import org.slf4j.LoggerFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import utils.InfluxDBSink




object DataStreamJob {
  // Set up the logger using SLF4J
  val LOG = LoggerFactory.getLogger(DataStreamJob.getClass)
  private val logger = LoggerFactory.getLogger(this.getClass)
  LOG.error("Picked picked")

  def main(args: Array[String]): Unit = {
    // Sets up the execution environment, which is the main entry point
    // to building Flink applications.
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val kafkaSourceTopic = "crypto_exchange_rates"
    val kafkaBootstrapServers = "localhost:9092"

    // Provide TypeInformation for CryptoData
    implicit val cryptoDataTypeInfo: TypeInformation[CryptoData] =
      TypeInformation.of(classOf[CryptoData])

    val kafkaSource: KafkaSource[CryptoData] = KafkaSource.builder[CryptoData]
      .setBootstrapServers(kafkaBootstrapServers)
      .setTopics(kafkaSourceTopic)
      .setGroupId("crypto-stream-pipeline")
      .setValueOnlyDeserializer(new CryptoDataDeserializer)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setProperty("request.timeout.ms", "60000")
      .build()

    val stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    stream.print()

    // Parse, transform, and reduce data
    val parsedStream = stream
      .map(data => DataParser.process(data))
      .filter(data => data != null)
      .keyBy(_.currency)
//          .timeWindow(org.apache.flink.streaming.api.windowing.time.Time.minutes(5))
//          .reduce(DataReducer.reduce _)
//          .writeAsText("output.txt")

        // Sink: Print to console (or send to other sinks like a database)
//        parsedStream.print()

    // Sink: Write to InfluxDB
    val influxDbUrl = "http://localhost:8086"
    val token: String = "ChJENYnBK5dxMWhWJpbcUGBFtlAvTWJHEJJa6Eo-U3LIhddWPkjtnev5JbE7N4ccLJOeXdZ-JXUVNpIqQozQnA=="
    val org = "crypto.org"
    val bucket = "crypto-data"

    // Sink
    parsedStream.addSink(new InfluxDBSink(influxDbUrl, token, org, bucket)).name("InfluxDB Sink")




    // Configure Elasticsearch sink
    val httpHosts = new HttpHost("localhost", 9200, "http")

    val esSinkBuilder = new Elasticsearch7SinkBuilder[CryptoData]()
      .setHosts(httpHosts)
      .setEmitter(new CryptoElasticsearchEmitter())
      .setBulkFlushMaxActions(1)
      .setBulkFlushInterval(1000)

    //Add Elasticsearch sink to the stream
    parsedStream.sinkTo(esSinkBuilder.build()).name("Elasticsearch Sink")



    // Execute the Flink job
    env.execute("Flink Scala Crpto Analysis")
  }

  class CryptoElasticsearchEmitter extends ElasticsearchEmitter[CryptoData] with Serializable {
    @transient private lazy val dateFormatter = DateTimeFormatter.ISO_INSTANT
    @transient private lazy val mapper = new ObjectMapper()

    def formatTimestamp(timestamp: Long): String = {
      try {
        java.time.Instant.ofEpochMilli(timestamp)
          .atOffset(ZoneOffset.UTC)
          .format(dateFormatter)
      } catch {
        case e: Exception =>
          // Log the error
          java.time.Instant.now().atOffset(ZoneOffset.UTC).format(dateFormatter)
          throw new RuntimeException(s"Failed to format date: ${timestamp}", e)
      }
    }

    def validateData(element: CryptoData): Boolean = {
      element != null &&
        element.currency != null &&
        element.currency.nonEmpty &&
        element.rates != null &&
        element.timestamp > 0
    }

    override def emit(element: CryptoData, context: SinkWriter.Context, indexer: RequestIndexer): Unit = {
      try {
        if (!validateData(element)) {
          throw new RuntimeException(s"Element: ${element} failed validation")
          return
        }
        val documentId = UUID.randomUUID().toString

       // Convert rates to Double
        val numericRates = element.rates.map { case (key, value) =>
          key -> value.toDouble
        }

        val objectMapper = new ObjectMapper()
        val parentNode: ObjectNode = objectMapper.createObjectNode()
        val ratesNode: ObjectNode = objectMapper.valueToTree(numericRates.asJava)

        LOG.info(s"[+] RATES: ${ratesNode}")
        logger.info(s"[+] RATES: ${ratesNode}")

        // Create JSON using Jackson
        val jsonNode = mapper.createObjectNode()
        jsonNode.put("currency", element.currency)
        jsonNode.set("rates", ratesNode)
        jsonNode.put("timestamp", formatTimestamp((element.timestamp)))

        val request = Requests.indexRequest()
          .index("crypto-rates")
          .id(documentId)
          .source(jsonNode.toString, XContentType.JSON)

        indexer.add(request)
      } catch {
        case e: Exception =>
          // Log the error
          throw new RuntimeException(s"Failed to emit crypto data: ${element}", e)
      }
    }
    override def open(): Unit = {}
    override def close(): Unit = {}
  }

}