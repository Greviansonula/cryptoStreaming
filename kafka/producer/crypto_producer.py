import json
import requests
from confluent_kafka import SerializingProducer
from datetime import datetime
import time


def get_crypto_prices():
    url = 'https://api.coinbase.com/v2/exchange-rates?currency=BTC'
    response = requests.get(url)
    ex_rates = json.loads(response.text)
    # return ex_rates["data"]
    return {
    "currency": "BTC",
    "rate": 49000.5
}

def delivery_report(err, msg):
    """
    Callback to handle the result of message delivery.
    
    Args:
        err: An error object if the message delivery failed, otherwise None.
        msg: The message object containing details about the delivered message.
    """
    if err:
        # Log delivery failure with additional details
        print(f"[ERROR] Message delivery failed: {err}")
    else:
        # Log successful delivery with topic, partition, and offset
        print(f"[INFO] Message delivered to topic: '{msg.topic()}', "
              f"partition: {msg.partition()}, offset: {msg.offset()}")

def main():
    topic = 'crypto_exchange_rates'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })

    curr_time = datetime.now()

    while True: #(datetime.now() - curr_time).seconds < 1200:
        try: 
            ex_rates = get_crypto_prices()
            # ex_rates['timestamp'] = datetime.now().isoformat(timespec='milliseconds')
            ex_rates['timestamp'] = int(datetime.now().timestamp() * 1000)

            print(ex_rates)

            producer.produce(
                topic,
                key=json.dumps({"currency": ex_rates['currency']}),  # Serialize key
                value=json.dumps(ex_rates),  # Serialize value
                on_delivery=delivery_report
            )
            producer.poll(0)

            # wait for 5 seconds
            # time.sleep(3)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()

