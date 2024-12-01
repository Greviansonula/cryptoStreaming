package Deserializer

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import Dto.CryptoData  // This import will now work
import scala.collection.JavaConverters._


class CryptoDataDeserializer extends DeserializationSchema[CryptoData] {
  private val objectMapper = new ObjectMapper()

  override def deserialize(message: Array[Byte]): CryptoData = {
    val node: JsonNode = objectMapper.readTree(message)

    val ratesNode = node.get("rates")
    val rates: Map[String, String] = ratesNode.fields().asScala.map { entry =>
      entry.getKey -> entry.getValue.asText()
    }.toMap

    CryptoData(
      currency = node.get("currency").asText(),
      rates = rates,
      timestamp = node.get("timestamp").asLong()
    )
  }

  override def isEndOfStream(nextElement: CryptoData): Boolean = false

  override def getProducedType: TypeInformation[CryptoData] = {
    TypeInformation.of(classOf[CryptoData])
  }
}