import json, time, requests, socket
from kafka import KafkaProducer

# Wait until Kafka responds
host, port = "kafka", 9092
for i in range(30):
    try:
        with socket.create_connection((host, port), timeout=2):
            print(f"✅ Kafka reachable on attempt {i+1}")
            break
    except Exception:
        print(f"⏳ Waiting for Kafka... attempt {i+1}")
        time.sleep(2)
else:
    raise RuntimeError("Kafka broker not reachable")

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=f"{host}:{port}",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
print("✅ Connected to Kafka broker")

# List of cities
cities = [
    {"name": "London", "lat": 51.5072, "lon": -0.1276},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Tokyo", "lat": 35.6895, "lon": 139.6917},
]

def get_weather(city):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current_weather=true"
    try:
        r = requests.get(url, timeout=5)
        if r.ok:
            data = r.json().get("current_weather", {})
            return {
                "city": city["name"],
                "temperature_c": data.get("temperature"),
                "wind_speed_ms": data.get("windspeed"),
                "timestamp": data.get("time"),
            }
    except Exception as e:
        print(f"⚠️ API error for {city['name']}: {e}")
    return None

while True:
    for city in cities:
        weather = get_weather(city)
        if weather:
            producer.send("weather_stream", weather)
            print("sent:", weather)
        else:
            print(f"⚠️ Skipped city {city['name']} (no data)")
    producer.flush()
    time.sleep(30)
