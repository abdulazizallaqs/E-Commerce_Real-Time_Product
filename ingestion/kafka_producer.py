import json
import time
from datetime import datetime
import random
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

TOPIC_NAME = 'product-updates'

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'], client_id='ecom-admin')

try:
    existing_topics = admin_client.list_topics()
    if TOPIC_NAME not in existing_topics:
        topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Created topic: {TOPIC_NAME}")
    else:
        print(f"Topic already exists: {TOPIC_NAME}")
except Exception as exc:
    print(f"Could not create topic automatically: {exc}")
finally:
    admin_client.close()

categories = ['Electronics', 'Home & Kitchen', 'Books', 'Sports']
product_names = {
    'Electronics': ['Wireless Mouse', 'Mechanical Keyboard', '4K Monitor', 'USB-C Cable'],
    'Home & Kitchen': ['Coffee Maker', 'Air Fryer', 'Blender', 'Chef Knife'],
    'Books': ['Python Clean Code', 'Designing Data-Intensive Applications', 'AI Basics'],
    'Sports': ['Yoga Mat', 'Dumbbells Set', 'Water Bottle', 'Running Shoes']
}


def generate_mock_product():
    category = random.choice(categories)
    name = random.choice(product_names[category])
    return {
        "product_id": f"PROD_{random.randint(1000, 9999)}",
        "name": name,
        "description": f"High quality {name} from {category} category.",
        "price": round(random.uniform(10.0, 500.0), 2),
        "stock_quantity": random.randint(0, 150),
        "category": category,
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Starting mock product updates to Kafka...")
    try:
        while True:
            product_data = generate_mock_product()
            producer.send(TOPIC_NAME, value=product_data)
            print(f"Sent product: {product_data['name']} (price: {product_data['price']})")
            time.sleep(3)
    except KeyboardInterrupt:
        print("\nStopped producer.")
    finally:
        producer.close()