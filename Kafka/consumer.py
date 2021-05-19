from kafka import KafkaConsumer


consumer = KafkaConsumer('kafkaTest', bootstrap_servers=['localhost:9099'])

for message in consumer:
    print(message.value)