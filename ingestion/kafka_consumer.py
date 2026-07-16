import json
from kafka import KafkaConsumer
from jsonschema import validate, ValidationError

with open('ingestion/schemas/product_schema.json', 'r', encoding='utf-8') as f:
    schema = json.load(f)

consumer = KafkaConsumer(
    'product-updates',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='inventory-validator-group',
    value_serializer=lambda x: json.loads(x.decode('utf-8'))
)


def validate_message(message_data):
    try:
        validate(instance=message_data, schema=schema)
        return True, "Valid"
    except ValidationError as err:
        return False, err.message


if __name__ == "__main__":
    print("Waiting for product messages to validate...")
    try:
        for message in consumer:
            product = message.value
            is_valid, reason = validate_message(product)

            if is_valid:
                print(f"[valid] {product['product_id']} ({product['name']}) matches the schema")
            else:
                print(f"[invalid] {product['product_id']} rejected: {reason}")
    except KeyboardInterrupt:
        print("\nStopped consumer.")
    finally:
        consumer.close()