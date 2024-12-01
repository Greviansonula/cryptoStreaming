package utils

import Dto.CryptoData
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.configuration.Configuration
import scalaj.http._
import org.slf4j.LoggerFactory

class InfluxDBSink(influxDbUrl: String, token: String, org: String, bucket: String)
  extends RichSinkFunction[CryptoData] {

  private val log = LoggerFactory.getLogger(this.getClass)

  private var writeUrl: String = _

  def formatCryptoDataToInfluxLineProtocol(data: CryptoData): String = {
    val measurement = "crypto_rates" // Measurement name
    val tag = s"currency=${data.currency}" // Tag for currency
    val fields = data.rates.map { case (crypto, rate) => s"$crypto=$rate" }.mkString(",") // Map rates to fields
    val timestamp = data.timestamp // Use provided timestamp

    s"$measurement,$tag $fields $timestamp"
  }

  override def open(parameters: Configuration): Unit = {
    writeUrl = s"$influxDbUrl/api/v2/write?org=$org&bucket=$bucket"
  }

  override def invoke(value: CryptoData, context: SinkFunction.Context): Unit = {
    val data = formatCryptoDataToInfluxLineProtocol(value)
//    val data = s"measurement,tag=value field=${value.field}"
    val response = Http(writeUrl)
      .postData(data)
      .header("Authorization", s"Token $token")
      .header("Content-Type", "text/plain")
      .asString

    if (response.code == 204) {
      log.info(s"Successfully wrote to InfluxDB: ${response.body}")
    } else {
      log.error(s"Failed to write to InfluxDB: ${response.body}")
      throw new RuntimeException(s"Failed to write to InfluxDB: ${response.body}")
    }

//    if (response.code != 204) {
//      throw new RuntimeException(s"Failed to write to InfluxDB: ${response.body}")
//    }
  }

  override def close(): Unit = {
    // Clean up resources if needed
  }
}