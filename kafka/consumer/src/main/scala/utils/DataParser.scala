package utils
import Dto.CryptoData

object DataParser {
  def process(data: CryptoData): CryptoData = {
    // Add any necessary processing logic here
    // For example, data validation, transformation, or enrichment
    CryptoData(
      timestamp = data.timestamp,
      currency = data.currency.toUpperCase, // Normalize currency to uppercase
      rate = data.rate
      // Add any additional fields or transformations needed
    )
  }

  def parse(rawMessage: String): CryptoData = {
    try {
      val parts = rawMessage.split(",")
      CryptoData(
        currency = parts(0).trim,
        rate = parts(1).toDouble,
        timestamp = parts(2).toLong
      )
    } catch {
      case ex: Exception =>
        println(s"Failed to parse message $rawMessage, error: $ex.getMessage}")
        CryptoData("UNKNOWN", 0.0, System.currentTimeMillis())
    }
  }
}
