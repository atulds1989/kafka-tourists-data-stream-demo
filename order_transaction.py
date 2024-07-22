from kafka import KafkaProducer, KafkaConsumer
import sys, os, time, json
from config import topics

TOUR_ORDER_TOPIC = topics["TOUR_ORDER_TOPIC"]
SLEEP_TIME = topics["SLEEP_TIME"]
TOUR_ORDER_CONFIRMED_TOPIC = topics["TOUR_ORDER_CONFIRMED_TOPIC"]
BOOTSTRAP_SERVERS = topics["BOOTSTRAP_SERVERS"]

def deserialize_message(val):
    try:
        return json.loads(val.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None

def serialize_message(val):
    try:
        return json.dumps(val).encode('utf-8')
    except json.JSONDecodeError as e:
        print(f"Error encoding JSON: {e}")
        return None

consumer = KafkaConsumer(
    TOUR_ORDER_TOPIC, 
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest', 
    enable_auto_commit=True,
    value_deserializer=deserialize_message
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS, 
    # value_serializer=lambda val: json.dumps(val).encode('utf-8')
    value_serializer=serialize_message
)

if __name__ == '__main__':
    print("**live streaming messages. To exit please press CTRL+C")

    try:
        while True:
            for stream in consumer:
                consumed_stream = stream.value
                if consumed_stream is None:
                    continue
                
                print(consumed_stream)

                data = {
                    "tourist_id": consumed_stream['tourist_id'],
                    "tourist_name": consumed_stream['name'],
                    "tour_order_id": consumed_stream['tour_order_id'],
                    "tourist_email": consumed_stream['email'],
                    "total_cost": consumed_stream['tour_cost']
                }
                
                print("order completed")
                producer.send(TOUR_ORDER_CONFIRMED_TOPIC, data)
                time.sleep(2)

    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)


