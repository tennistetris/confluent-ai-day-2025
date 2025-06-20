from confluent_kafka import Producer
import sys

# Configuration for Confluent Cloud
config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'THV3Q2775Y3Z3I5M',
    'sasl.password': 'LPUb0KrISli6HdCgUhCZp22LTApB+UtGWp7ihLTbULGbqerG9DKN/61ZaLIAJqiM',
}

producer = Producer(config)

# Read and publish image
image_path = sys.argv[1]
with open(image_path, "rb") as f:
    data = f.read()

producer.produce("raw-data", key=image_path.encode(), value=data)
producer.flush()
print(f"✅ Published {image_path} ({len(data)} bytes)")
