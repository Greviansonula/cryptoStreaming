package utils

object DataReducer {

  def reduce(data1: CryptoData, data2: CryptoData): CryptoData = {
    // Example: Average the prices during the window
    CryptoData(
      currency = data1.currency,
      price = (data1.price + data2.price) / 2,
      timestamp = Math.max(data1.timestamp, data2.timestamp)
    )
  }
}

