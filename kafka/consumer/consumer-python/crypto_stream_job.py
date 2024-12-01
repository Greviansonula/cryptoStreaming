from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import Time
from pyflink.common.watermark_strategy import WatermarkStrategy


# print(help(FlinkKafkaConsumer))

# Define the data structure
class CryptoData:
    def __init__(self, currency: str, rate: float, timestamp: int):
        self.currency = currency
        self.rate = rate
        self.timestamp = timestamp

# Utility functions
def parse_data(raw_data):
    """
    Parses raw Kafka message (JSON string) into a CryptoData object.
    """
    import json
    data = json.loads(raw_data)
    return CryptoData(currency=data["currency"], rate=data["rate"], timestamp=data["timestamp"])

def reduce_data(data1, data2):
    """
    Aggregates two CryptoData objects by taking the average rate and updating the timestamp.
    """
    return CryptoData(
        currency=data1.currency,
        rate=(data1.rate + data2.rate) / 2,
        timestamp=max(data1.timestamp, data2.timestamp)
    )

def map_to_string(data):
    """
    Converts CryptoData object to string for printing or other sinks.
    """
    return f"Currency: {data.currency}, Rate: {data.rate}, Timestamp: {data.timestamp}"

# Main job
def main():
    # Set up the Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)

    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "crypto_exchange_rates"
    kafka_group_id = "crypto-stream-group"

    # Kafka Consumer
    kafka_consumer = FlinkKafkaConsumer(
        topics=kafka_topic,
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": kafka_bootstrap_servers,
            "group.id": kafka_group_id,
            "request.timeout.ms": "60000"
        }
    )

    # Stream from Kafka
    stream = env.add_source(kafka_consumer)

    # Parse, key by currency, window by 5 minutes, and reduce
    parsed_stream = (
        stream
        .map(parse_data)  # Parse raw JSON into CryptoData objects
        .key_by(lambda crypto: crypto.currency)  # Key by currency
        .time_window(Time.minutes(5))  # Tumbling 5-minute window
        .reduce(reduce_data)  # Reduce by averaging rates
    )

    # Sink: Print to console
    parsed_stream.map(map_to_string).print()

    # Execute the Flink job
    env.execute("Flink Python Crypto Analysis")

if __name__ == "__main__":
    print("Starting")
    main()
