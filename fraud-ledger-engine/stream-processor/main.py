import os
import sys
import json
import signal
import logging
import httpx
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from redis_client import RedisFeatureStore

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("stream-processor")

# Load Configurations
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "fraud-stream-processor")
SRC_TOPIC = os.getenv("SRC_TOPIC", "raw-transactions")
APPROVED_TOPIC = os.getenv("APPROVED_TOPIC", "approved-transactions")
FLAGGED_TOPIC = os.getenv("FLAGGED_TOPIC", "flagged-transactions")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

ML_SERVING_URL = os.getenv("ML_SERVING_URL", "http://localhost:8000/predict")

# Keep track of active loop status
running = True

def handle_shutdown(signum, frame):
    global running
    logger.info("Termination signal received. Shutting down stream processor...")
    running = False

# Register signal handlers for graceful exit
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def delivery_report(err, msg):
    """Callback for Kafka producer to report message delivery status."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def main():
    global running

    # 1. Initialize Redis Feature Store
    try:
        feature_store = RedisFeatureStore(host=REDIS_HOST, port=REDIS_PORT)
    except Exception as e:
        logger.critical(f"Could not connect to Redis Feature Store: {e}")
        sys.exit(1)

    # 2. Configure Kafka Consumer
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKERS,
        'group.id': CONSUMER_GROUP,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False, # Manual offset commits after processing ensures no lost transactions!
    }
    
    # 3. Configure Kafka Producer
    producer_config = {
        'bootstrap.servers': KAFKA_BROKERS,
        'acks': 'all', # Guarantee durable writes to Kafka replicas
        'linger.ms': 5, # Batch messages slightly to optimize throughput
    }

    try:
        consumer = Consumer(consumer_config)
        producer = Producer(producer_config)
    except KafkaException as ke:
        logger.critical(f"Kafka client initialization failed: {ke}")
        sys.exit(1)

    consumer.subscribe([SRC_TOPIC])
    logger.info(f"Subscribed to Kafka source topic '{SRC_TOPIC}'")

    # 4. Initialize HTTP persistent client (connection pool) to call FastAPI
    # Using a persistent client avoids TCP handshake overhead on every iteration!
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=50)
    with httpx.Client(timeout=1.0, limits=limits) as http_client:
        logger.info(f"Connected to ML Serving API at {ML_SERVING_URL}")
        
        while running:
            # Poll Kafka for raw transaction events (100ms timeout)
            msg = consumer.poll(0.1)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event, not a critical error
                    continue
                else:
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    continue

            try:
                # Parse incoming message
                raw_payload = msg.value().decode('utf-8')
                tx = json.loads(raw_payload)

                txn_id = tx.get("txn_id")
                user_id = tx.get("user_id")
                amount = tx.get("amount")
                device_id = tx.get("device_id")

                if not all([txn_id, user_id, amount, device_id]):
                    logger.warning(f"Malformed transaction event received: {tx}. Committing offset & skipping.")
                    consumer.commit(msg, asynchronous=True)
                    continue

                # 5. Fetch sliding-window counts from Redis Feature Store
                count_5m, velocity_5m = feature_store.get_and_update_velocity(
                    user_id=user_id,
                    txn_id=txn_id,
                    amount=amount
                )

                # 6. Execute ML Inference prediction request
                prediction_payload = {
                    "txn_id": txn_id,
                    "user_id": user_id,
                    "amount": float(amount),
                    "device_id": device_id,
                    "count_5m": count_5m,
                    "velocity_5m": float(velocity_5m)
                }

                decision = "FLAG"
                fraud_prob = 1.0

                try:
                    # Send payload to the low-latency FastAPI server
                    response = http_client.post(ML_SERVING_URL, json=prediction_payload)
                    
                    if response.status_code == 200:
                        resp_data = response.json()
                        fraud_prob = resp_data.get("fraud_probability", 1.0)
                        decision = resp_data.get("decision", "FLAG")
                    else:
                        logger.error(f"ML serving returned HTTP status {response.status_code} for txn {txn_id}")
                        # Fallback heuristic logic if ML server returns error code
                        decision = "FLAG" if amount > 500.0 else "APPROVE"
                        fraud_prob = 0.90 if decision == "FLAG" else 0.40

                except httpx.HTTPError as http_err:
                    logger.error(f"ML Inference connection failed: {http_err}. Triggering defensive fallback rules.")
                    # Defensive fallback: block high-value transactions automatically
                    decision = "FLAG" if amount > 500.0 else "APPROVE"
                    fraud_prob = 0.90 if decision == "FLAG" else 0.40

                # 7. Route transaction payload downstream to correct topic
                enrichment_payload = {
                    **tx,
                    "count_5m": count_5m,
                    "velocity_5m": velocity_5m,
                    "fraud_score": fraud_prob,
                    "decision": decision,
                    "processed_at": now_timestamp()
                }
                
                output_payload_bytes = json.dumps(enrichment_payload).encode('utf-8')
                
                # Maintain partitioning key alignment by routing on UserID
                target_topic = FLAGGED_TOPIC if decision == "FLAG" else APPROVED_TOPIC
                
                producer.produce(
                    topic=target_topic,
                    key=user_id.encode('utf-8'),
                    value=output_payload_bytes,
                    callback=delivery_report
                )

                # Poll producer queue to trigger callbacks and flush buffer asynchronously
                producer.poll(0)

                # 8. Commit Kafka consumer offset
                consumer.commit(msg, asynchronous=True)
                
                logger.info(
                    f"Processed: {txn_id} | User: {user_id} | Amount: ${amount:.2f} | "
                    f"Count(5m): {count_5m} | Velocity(5m): ${velocity_5m:.2f} | "
                    f"Score: {fraud_prob:.4f} | -> {target_topic}"
                )

            except Exception as e:
                logger.error(f"Error handling stream transaction event: {e}", exc_info=True)

    # 9. Clean Shutdown
    logger.info("Closing Kafka consumer and producer...")
    try:
        consumer.close()
        producer.flush(timeout=5)
    except Exception as shutdown_err:
        logger.error(f"Error during graceful consumer/producer closure: {shutdown_err}")
        
    logger.info("Stream processor stopped successfully.")

def now_timestamp() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

if __name__ == "__main__":
    main()
