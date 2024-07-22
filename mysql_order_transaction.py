import mysql.connector
from kafka import KafkaConsumer, KafkaProducer
import json, os, sys, time
from config import topics
from dotenv import load_dotenv
from database_operations import Database_ops
load_dotenv()

TOUR_ORDER_TOPIC = topics["TOUR_ORDER_TOPIC"]
TOUR_ORDER_CONFIRMED_TOPIC = topics["TOUR_ORDER_CONFIRMED_TOPIC"]
BOOTSTRAP_SERVERS = topics["BOOTSTRAP_SERVERS"]

def deserialize_message(val):
    try:
        return json.loads(val.decode('utf-8'))
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None
    
def serializer_message(val):
    try:
        return json.dumps(val).encode("utf-8")
    except json.JSONDecodeError as e:
        print(f"Error in encoding JSON : {e}")
        return None
    

consumer = KafkaConsumer(
    TOUR_ORDER_TOPIC, 
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest', 
    enable_auto_commit=True,
    value_deserializer=deserialize_message
)

producer = KafkaProducer(
                    bootstrap_servers = BOOTSTRAP_SERVERS,
                    value_serializer = serializer_message

                )

if __name__ == '__main__':
    table_name = 'tour_order_confirmed_detail'
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        tourist_id INT NOT NULL,
                        tourist_name VARCHAR(255) NOT NULL,
                        tour_order_id VARCHAR(50),
                        tourist_email VARCHAR(255) NOT NULL,
                        total_cost DECIMAL(50, 2) NOT NULL
                    );"""
    obj = Database_ops()

    obj.create_table(query=create_table_query)

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

                producer.send(TOUR_ORDER_CONFIRMED_TOPIC, data)
                time.sleep(0)
                insert_query = f"""INSERT INTO {table_name} (tourist_id, tourist_name, tour_order_id, tourist_email, total_cost) 
                        VALUES ('{data['tourist_id']}', '{data['tourist_name']}', '{data['tour_order_id']}', '{data['tourist_email']}','{data['total_cost']}')"""
                obj.insert_data(query=insert_query)
            
    except Exception as e:
        print(f"Error: {e}")
    except KeyboardInterrupt:
        obj.close_connection()
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)





