import streamlit as st
import pandas as pd
from confluent_kafka import Producer, Consumer, KafkaError
import json
import uuid
import toml
from jsonschema import validate, ValidationError

# Load secrets from secrets.toml
with open('secrets.toml', 'r') as f:
    secrets = toml.load(f)

# Confluent Cloud configuration
KAFKA_BOOTSTRAP_SERVERS = secrets['confluent_cloud']['KAFKA_BOOTSTRAP_SERVERS']
KAFKA_API_KEY = secrets['confluent_cloud']['KAFKA_API_KEY']
KAFKA_API_SECRET = secrets['confluent_cloud']['KAFKA_API_SECRET']
KAFKA_TOPIC = secrets['confluent_cloud']['KAFKA_TOPIC']

# Define the schema
aml_alert_schema = {
  "type": "record",
  "namespace": "com.yourcompany.aml",
  "name": "AmlAlert",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "timestamp", "type": "string"},
    {"name": "account_id", "type": "string"},
    {"name": "alert_type", "type": "string"}
  ]
}

# Initialize Kafka producer
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET,
}

producer = Producer(producer_config)

# Function to send data to Kafka
def send_to_kafka(data):
    try:
        validate(instance=data, schema=aml_alert_schema)
        producer.produce(KAFKA_TOPIC, key=str(uuid.uuid4()), value=json.dumps(data))
        producer.flush()
    except ValidationError as e:
        st.error(f"Data validation error: {e.message}")

# Streamlit app
st.title("Real-time AML Alerts System")

# File uploader
uploaded_file = st.file_uploader("Choose an Excel file", type="xlsx")
if uploaded_file:
    df = pd.read_excel(uploaded_file)
    st.write(df)

    # Send data to Kafka
    for index, row in df.iterrows():
        transaction = row.to_dict()
        transaction["transaction_id"] = str(uuid.uuid4())
        send_to_kafka(transaction)

# Kafka consumer setup
consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': KAFKA_API_KEY,
    'sasl.password': KAFKA_API_SECRET,
    'group.id': 'aml_alerts_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])

st.header("Real-time Alerts")

# Read messages from Kafka and display them in Streamlit
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            st.error(f"Consumer error: {msg.error()}")
            break

    alert = json.loads(msg.value().decode('utf-8'))
    if 'transaction_id' in alert:
        st.json(alert)
        if st.button("Acknowledge", key=alert['transaction_id']):
            st.write(f"Acknowledged transaction {alert['transaction_id']}")
