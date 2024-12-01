package Deserializer

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import Dto.CryptoData  // This import will now work

class CryptoDataDeserializer extends DeserializationSchema[CryptoData] {
  private val objectMapper = new ObjectMapper()

  override def deserialize(message: Array[Byte]): CryptoData = {
    val node: JsonNode = objectMapper.readTree(message)

    CryptoData(
      currency = node.get("currency").asText(),
      rate = node.get("rate").asDouble(),
      timestamp = node.get("timestamp").asLong()
    )
  }

  override def isEndOfStream(nextElement: CryptoData): Boolean = false

  override def getProducedType: TypeInformation[CryptoData] = {
    TypeInformation.of(classOf[CryptoData])
  }
}