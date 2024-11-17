import json
import requests
from confluent_kafka import SerializingProducer
from datetime import datetime
import time


def get_crypto_prices():
    url = 'https://api.coinbase.com/v2/exchange-rates?currency=BTC'
    response = requests.get(url)
    ex_rates = json.loads(response.text)
    return ex_rates

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic} [{msg.partition()}]')

def main():
    topic = 'crypto_exchange_rates'
    producer = SerializingProducer({
        'bootstrap.servers': 'localhost:9092'
    })

    curr_time = datetime.now()

    while (datetime.now() - curr_time).seconds < 120:
        try: 
            ex_rates = get_crypto_prices()
            ex_rates['timestamp'] = datetime.now().isoformat(timespec='milliseconds')

            print(ex_rates)

            producer.produce(topic, 
                            key=json.dumps(ex_rates),
                            on_delivery=delivery_report
                            )
            producer.poll(0)

            # wait for 5 seconds
            time.sleep(5)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()

