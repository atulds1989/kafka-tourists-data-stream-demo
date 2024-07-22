from kafka import KafkaProducer
from time import sleep
import json
from datetime import datetime
from kafka import KafkaConsumer
import random
from faker import Faker

faker = Faker()

places_list  = ["Mumbai", "delhi", "Indore","Nagpur","Banglore","Pune","Jaipur","Jodhpur","Dehradoon", "Shimla","ladakh","Tirupati","Ahmedabad"]


def get_tourist_data():

    data = {
        'tourist_id':faker.unique.random_int(min=1, max=100000),
        'name':faker.name(),
        'address':faker.address(),
        'phone':faker.phone_number(),
        'tour_cost':faker.random_int(5000,100000),
        'tour_order_id':faker.uuid4(),
        'places':','.join(random.sample(places_list, 4))
    }

    email = f'''{"".join(data['name'].split())}@gmail.com'''
    data['email'] = email.lower()

    return data


if __name__ == "__main__":
    print(get_tourist_data())