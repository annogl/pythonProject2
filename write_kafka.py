from kafka import KafkaProducer
from json import dumps

def writing_kafka_file(table):
    producer = KafkaProducer(bootstrap_servers='localhost:461011', value_serializer=lambda x: dumps(x).encode('utf-8'))

    for e in range(10):
        producer.send(table[e])
        producer.flush()
        producer.close()