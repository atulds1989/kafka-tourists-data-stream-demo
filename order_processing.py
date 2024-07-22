from kafka import KafkaProducer
from config import topics
import json
import time
import sys
import os
from data import get_tourist_data
from dotenv import load_dotenv
import mysql.connector
from database_operations import Database_ops
load_dotenv()

TOUR_ORDER_TOPIC = topics["TOUR_ORDER_TOPIC"]
TOUR_ORDER_CONFIRMED_TOPIC = topics["TOUR_ORDER_CONFIRMED_TOPIC"]
BOOTSTRAP_SERVERS = topics["BOOTSTRAP_SERVERS"]


def serialize_message(val):
    try:
        return json.dumps(val).encode('utf-8')
    except json.JSONDecodeError as e:
        print(f"Error encoding JSON: {e}")
        return None

producer = KafkaProducer(bootstrap_servers = topics['BOOTSTRAP_SERVERS'],
                        #  value_serializer=lambda val:json.dumps(val).encode("utf-8")
                        value_serializer=serialize_message
                        )


if __name__=="__main__":
    table_name = 'tourists_detail'
    create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        tourist_id INT NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        address VARCHAR(255),
                        phone VARCHAR(100),
                        tour_cost DECIMAL(20, 2),
                        tour_order_id VARCHAR(50) NOT NULL,
                        places TEXT
                    );
                    """
    obj = Database_ops()
    obj.create_table(query=create_table_query)
    try:
          while True:
               registered_tourist = get_tourist_data()
               print(registered_tourist)
               data = {
                        'tourist_id':registered_tourist['tourist_id'],
                        'name':registered_tourist['name'],
                        'address':registered_tourist['address'],
                        'phone':registered_tourist['phone'],
                        'tour_cost':registered_tourist['tour_cost'],
                        'tour_order_id':registered_tourist['tour_order_id'],
                        'places':registered_tourist['places']
                    }
               insert_query = f"""INSERT INTO {table_name} (tourist_id, name, address, phone, tour_cost, tour_order_id, places) 
                                VALUES ('{data['tourist_id']}', '{data['name']}', '{data['address']}',
                                '{data['phone']}','{data['tour_cost']}', '{data['tour_order_id']}', '{data['places']}')"""
        
               obj.insert_data(query=insert_query)
               producer.send(topics['TOUR_ORDER_TOPIC'], registered_tourist)
               time.sleep(0)
    except KeyboardInterrupt:
        obj.close_connection()
        print("keyboard interrupted")
        try:
              sys.exit(0)
        except SystemExit:
              os._exit(0)


