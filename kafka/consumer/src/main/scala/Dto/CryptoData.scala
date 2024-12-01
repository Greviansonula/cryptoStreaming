package Dto

case class CryptoData(currency: String, rates: Map[String, String], timestamp: Long) extends Serializable