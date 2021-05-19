from kafka import KafkaProducer
import json
from json import dumps



producer = KafkaProducer(bootstrap_servers = ['localhost:9099'])
f = open('Gargoyles.txt', 'r')
readline = f.readlines()

# print(readline)

for x in range(len(readline)):
    producer.send('kafkaTest', json.dumps(readline).encode('utf-8'))
print("working")
producer.flush()

