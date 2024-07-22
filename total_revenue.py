from kafka import KafkaConsumer, KafkaProducer
import sys, os, time
from config import topics
from order_transaction import serialize_message, deserialize_message, TOUR_ORDER_CONFIRMED_TOPIC, TOUR_ORDER_TOPIC


consumer = KafkaConsumer(
    TOUR_ORDER_CONFIRMED_TOPIC,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=deserialize_message
)

total_tourists = 0
total_income = 0

if __name__=='__main__':
    print("**live streaming messages. To exit please press CTRL+C")

    try:
        while True:
            for stream in consumer:
                consumed_stream = stream.value

                if consumed_stream is None:
                    continue

                # print(consumed_stream)

                total_cost = consumed_stream['total_cost']
                total_income+=total_cost
                total_tourists+=1

                print(f"Total tourists till today : {total_tourists}")
                print(f"Total Income till today : {total_income}")

    except KeyboardInterrupt:
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)






