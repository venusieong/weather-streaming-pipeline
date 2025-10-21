from kafka import KafkaConsumer
import json
c = KafkaConsumer(
    'weather_stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)
for msg in c:
    print(msg.value)
