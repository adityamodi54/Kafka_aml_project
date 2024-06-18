import streamlit as st
import pandas as pd
import re
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaError
import json
import uuid
from jsonschema import validate, ValidationError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load secrets from Streamlit's secrets management
secrets = st.secrets["confluent_cloud"]

# Confluent Cloud configuration
KAFKA_BOOTSTRAP_SERVERS = secrets["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_API_KEY = secrets["KAFKA_API_KEY"]
KAFKA_API_SECRET = secrets["KAFKA_API_SECRET"]
KAFKA_TOPIC = secrets["KAFKA_TOPIC"]

# Define the schema
aml_alert_schema = {
    "type": "object",
    "properties": {
        "transaction_id": {"type": "string"},
        "amount": {"type": "number"},
        "currency": {"type": "string"},
        "timestamp": {"type": "string"},
        "account_id": {"type": "string"},
        "alert_type": {"type": "string"}
    },
    "required": ["transaction_id", "amount", "currency", "timestamp", "account_id", "alert_type"]
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
        logger.info(f"Sent data to Kafka: {data}")
    except ValidationError as e:
        logger.error(f"Data validation error: {e.message} (row: {data})")
        return f"Data validation error: {e.message} (row: {data})"
    except Exception as e:
        logger.error(f"Failed to send message: {e}")
        return f"Failed to send message: {e}"
    return None

# Function to check email format using regular expressions
def is_valid_email(email):
    regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(regex, email) is not None

# Function to check phone number format
def is_valid_phone(phone):
    regex = r'^\+?[0-9]{10,15}$'
    cleaned_phone = re.sub(r'[^0-9]', '', phone.split('x')[0])
    return re.match(regex, cleaned_phone) is not None

# Function to check if card expiry date is valid and not in the past
def is_valid_expiry(date_str):
    try:
        expiry_date = datetime.strptime(date_str, "%m/%y")
        return expiry_date > datetime.now()
    except ValueError:
        return False

# Function to check if account number follows a valid format
def is_valid_account_number(account_number):
    regex = r'^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$'
    return re.match(regex, account_number) is not None

# Function to check if the timestamp is in a valid ISO 8601 format
def is_valid_timestamp(timestamp):
    try:
        datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        return True
    except ValueError:
        return False

# Function to check if card number is valid using the Luhn algorithm
def is_valid_card_number(card_number):
    card_number = str(card_number)  # Ensure card_number is a string
    card_number = re.sub(r'[^0-9]', '', card_number)
    def digits_of(n):
        return [int(d) for d in str(n)]
    digits = digits_of(card_number)
    checksum = 0
    odd_digits = digits[-1::-2]
    even_digits = digits[-2::-2]
    checksum += sum(odd_digits)
    for d in even_digits:
        checksum += sum(digits_of(d * 2))
    return checksum % 10 == 0

# List of valid account types for validation
valid_account_types = ["Savings", "Credit", "Checking"]

# Streamlit app title
st.title("Real-time AML Alerts System")

# File uploader widget to upload an Excel file
uploaded_file = st.file_uploader("Choose an Excel file", type="xlsx")
if uploaded_file:
    # Read the uploaded Excel file into a pandas DataFrame
    df = pd.read_excel(uploaded_file)
    
    # Print the column names to verify
    st.write("Column names in the uploaded file:", df.columns)
    logger.info(f"Column names in the uploaded file: {df.columns}")
    
    compliance_issues = []
    suspicious_transactions = []

    # Define the threshold amount for suspicious transactions
    suspicious_amount_threshold = 10000
    # Define high-risk countries for suspicious transactions
    high_risk_countries = ["Country1", "Country2"]

    # Ensure all columns have compatible types
    required_columns = [
        'Transaction ID', 'Account Number', 'Customer Email', 'Customer Phone',
        'Transaction Amount', 'Card Expiry', 'Currency', 'Status', 'Timestamp',
        'Account Type', 'Transaction Type', 'Available Balance', 'Customer Name', 'Card Number'
    ]

    # Check if all required columns are present
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        st.error(f"Missing required columns in the uploaded file: {missing_columns}")
        logger.error(f"Missing required columns in the uploaded file: {missing_columns}")
    else:
        df = df.astype({
            'Transaction ID': 'str',
            'Account Number': 'str',
            'Customer Email': 'str',
            'Customer Phone': 'str',
            'Transaction Amount': 'float',
            'Card Expiry': 'str',
            'Currency': 'str',
            'Status': 'str',
            'Timestamp': 'str',
            'Account Type': 'str',
            'Transaction Type': 'str',
            'Available Balance': 'float',
            'Customer Name': 'str',
            'Card Number': 'str'
        })

        # Iterate over each row in the DataFrame
        for idx, row in df.iterrows():
            issues = []
            suspicious = False

            # Perform various compliance checks and collect issues
            if not is_valid_account_number(row['Account Number']):
                issues.append("Invalid account number format")
            if not is_valid_email(row['Customer Email']):
                issues.append("Invalid email format")
            if not is_valid_phone(row['Customer Phone']):
                issues.append(f"Invalid phone number format ({row['Customer Phone']})")
            if row['Transaction Amount'] <= 0:
                issues.append(f"Transaction amount must be positive ({row['Transaction Amount']})")
            if not is_valid_expiry(row['Card Expiry']):
                issues.append(f"Card expiry date is invalid or past ({row['Card Expiry']})")
            if row['Currency'] not in ["USD", "EUR", "GBP"]:
                issues.append(f"Invalid currency code ({row['Currency']})")
            if row['Status'] not in ["Pending", "Completed", "Failed"]:
                issues.append(f"Invalid transaction status ({row['Status']})")
            if not is_valid_timestamp(row['Timestamp']):
                issues.append(f"Invalid timestamp format ({row['Timestamp']})")
            if row['Account Type'] not in valid_account_types:
                issues.append(f"Invalid account type ({row['Account Type']})")
            if row['Transaction Type'] not in ["Debit", "Credit"]:
                issues.append(f"Invalid transaction type ({row['Transaction Type']})")
            if row['Available Balance'] <= 0:
                issues.append(f"Available balance must be positive ({row['Available Balance']})")
            if not re.match(r'^[a-zA-Z\s]+$', row['Customer Name']):
                issues.append(f"Invalid customer name format ({row['Customer Name']})")
            if not is_valid_card_number(row['Card Number']):
                issues.append(f"Invalid card number ({row['Card Number']})")

            # Check for suspicious transactions
            if row['Transaction Amount'] > suspicious_amount_threshold:
                suspicious = True
                issues.append(f"Transaction amount exceeds suspicious threshold ({row['Transaction Amount']})")
            if 'Country' in row and row['Country'] in high_risk_countries:
                suspicious = True
                issues.append(f"Transaction involves high-risk country ({row['Country']})")

            if issues:
                compliance_issues.append({
                    "Transaction ID": row['Transaction ID'],
                    "Issues": "; ".join(issues),
                    "Reasons": issues  # Adding the issues as reasons
                })

            # Collect suspicious transactions
            if suspicious:
                suspicious_transactions.append({
                    **row.to_dict(),
                    "Reasons": [issue for issue in issues if "suspicious" in issue.lower()]
                })
            else:
                # Transform and send valid transaction to Kafka
                transaction = {
                    "transaction_id": str(uuid.uuid4()),
                    "amount": row['Transaction Amount'],
                    "currency": row['Currency'],
                    "timestamp": row['Timestamp'],
                    "account_id": row['Account Number'],
                    "alert_type": row['Transaction Type']
                }
                error = send_to_kafka(transaction)
                if error:
                    st.error(error)
                else:
                    logger.info(f"Transaction sent to Kafka: {transaction}")

        # Display the compliance issues found, if any
        if compliance_issues:
            st.write("Compliance Issues Found:")
            st.write(pd.DataFrame(compliance_issues))
            logger.info(f"Compliance Issues Found: {compliance_issues}")
        else:
            st.write("No compliance issues found!")
            logger.info("No compliance issues found!")

        # Display suspicious transactions found, if any
        if suspicious_transactions:
            st.write("Suspicious Transactions Found:")
            st.write(pd.DataFrame(suspicious_transactions))
            logger.info(f"Suspicious Transactions Found: {suspicious_transactions}")
        else:
            st.write("No suspicious transactions found!")
            logger.info("No suspicious transactions found!")

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
def display_messages():
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                st.error(f"Consumer error: {msg.error()}")
                logger.error(f"Consumer error: {msg.error()}")
                break

        alert = json.loads(msg.value().decode('utf-8'))
        if 'transaction_id' in alert:
            st.json(alert)
            logger.info(f"Received message: {alert}")
            if st.button("Acknowledge", key=alert['transaction_id']):
                st.write(f"Acknowledged transaction {alert['transaction_id']}")
                logger.info(f"Acknowledged transaction {alert['transaction_id']}")

# Add a button to start consuming messages
if st.button("Start Consuming Messages"):
    display_messages()
