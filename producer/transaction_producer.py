"""
producer/transaction_producer.py
---------------------------------
Simulates real-time credit card transaction events and publishes
them to a Kafka topic at configurable throughput.
"""

import json
import random
import time
import uuid
import logging
from datetime import datetime
from kafka import KafkaProducer
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


# ── Load config ───────────────────────────────────────────────────────────────
with open("config/config.yaml") as f:
    CONFIG = yaml.safe_load(f)

KAFKA_BROKER   = CONFIG["kafka"]["broker"]
TOPIC          = CONFIG["kafka"]["topic"]
TPS            = CONFIG["producer"]["tps"]           # transactions per second
RUN_DURATION   = CONFIG["producer"]["run_duration"]  # seconds; -1 = infinite


# ── Reference data ────────────────────────────────────────────────────────────
CARD_TYPES     = ["VISA", "MASTERCARD", "RUPAY", "AMEX"]
MERCHANT_CATS  = ["GROCERY", "FUEL", "RESTAURANT", "TRAVEL", "ECOMMERCE",
                  "UTILITIES", "HEALTHCARE", "ENTERTAINMENT"]
MERCHANTS      = ["Swiggy", "Zomato", "Amazon", "Flipkart", "IRCTC",
                  "BigBasket", "Reliance Fresh", "HP Petrol", "Apollo Pharmacy",
                  "BookMyShow", "Uber", "Ola", "MakeMyTrip", "Nykaa"]
CITIES         = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai",
                  "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"]
TXN_STATUSES   = ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED", "PENDING"]  # weighted


def generate_transaction() -> dict:
    """Generate one synthetic credit card transaction event."""
    amount = round(random.lognormvariate(5.5, 1.2), 2)  # realistic INR distribution
    card_type = random.choice(CARD_TYPES)
    merchant  = random.choice(MERCHANTS)
    city      = random.choice(CITIES)

    return {
        "transaction_id":   str(uuid.uuid4()),
        "card_id":          f"CARD_{random.randint(100000, 999999)}",
        "customer_id":      f"CUST_{random.randint(10000, 99999)}",
        "amount":           amount,
        "currency":         "INR",
        "card_type":        card_type,
        "merchant_name":    merchant,
        "merchant_category": random.choice(MERCHANT_CATS),
        "city":             city,
        "status":           random.choice(TXN_STATUSES),
        "is_international": random.random() < 0.05,        # 5% international
        "event_timestamp":  datetime.utcnow().isoformat(),
        "event_date":       datetime.utcnow().strftime("%Y-%m-%d"),
    }


def on_send_success(record_metadata):
    logger.debug(
        f"Delivered to {record_metadata.topic} "
        f"[partition={record_metadata.partition}, offset={record_metadata.offset}]"
    )


def on_send_error(exc):
    logger.error(f"Failed to deliver message: {exc}")


def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",               # wait for all replicas to acknowledge
        retries=3,
        linger_ms=10,             # batch messages for 10ms to improve throughput
    )

    logger.info(f"Producer started → topic: {TOPIC} @ {TPS} TPS")

    sent      = 0
    start     = time.time()
    interval  = 1.0 / TPS

    try:
        while True:
            if RUN_DURATION > 0 and (time.time() - start) >= RUN_DURATION:
                break

            txn = generate_transaction()
            producer.send(TOPIC, value=txn) \
                     .add_callback(on_send_success) \
                     .add_errback(on_send_error)

            sent += 1
            if sent % 500 == 0:
                elapsed = time.time() - start
                logger.info(f"Published {sent} events | Elapsed: {elapsed:.1f}s | "
                            f"Effective TPS: {sent/elapsed:.1f}")

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Producer shut down. Total events sent: {sent}")


if __name__ == "__main__":
    run_producer()
