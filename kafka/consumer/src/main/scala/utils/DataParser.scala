package utils

case class CryptoData(currency: String, price: Double, timestamp: Long)

object DataParser {
  def parse(rawMessage: String): CryptoData = {
    try {
      val parts = rawMessage.split(",")
      CryptoData(
        currency = parts(0).trim,
        price = parts(1).toDouble,
        timestamp = parts(2).toLong
      )
    } catch {
      case ex: Exception =>
        println(s"Failed to parse message $rawMessage, error: $ex.getMessage}")
        CryptoData("UNKNOWN", 0.0, System.currentTimeMillis())
    }
  }
}
