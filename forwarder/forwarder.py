"""forwarder: consumes voltage-spike events from a Fabric Eventstream custom endpoint
(via the Kafka protocol) and republishes them to the Azure Event Hub EH-target (via AMQP).

The Kafka protocol is used for the SOURCE because the Event Hubs/AMQP protocol's
partition-level CBS auth is consistently rejected by this Fabric custom endpoint
(com.microsoft:auth-failed), even though the same connection string authenticates
fine for AMQP metadata operations. Kafka's SASL_SSL/PLAIN auth is a different code
path that does not hit this issue.

Usage (env vars):
  SOURCE_CONNECTION_STRING   connection string for the Fabric custom endpoint
                             (Event Hub protocol format: Endpoint=sb://<host>/;
                             SharedAccessKeyName=...;SharedAccessKey=...;EntityPath=<topic>)
                             - bootstrap server and topic are parsed out of it.
  SOURCE_CONSUMER_GROUP      Kafka consumer group / EventHub consumer group (default: $Default)
  TARGET_CONNECTION_STRING   connection string for EH-target (fabric-send-policy)
  DURATION_SECONDS           how long to consume/forward per run (default: 60)
  KAFKA_AUTO_OFFSET_RESET    "earliest" or "latest" (default: earliest)
"""
import json
import os
import re
import time
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from confluent_kafka import Consumer, KafkaException

forwarded_count = 0


def _parse_source_connection_string(connection_string: str) -> tuple[str, str]:
    """Extract (bootstrap_server, topic) from an Event-Hub-protocol connection string."""
    match = re.search(r"Endpoint=sb://([^/;]+)", connection_string)
    if not match:
        raise ValueError("SOURCE_CONNECTION_STRING is missing an Endpoint=sb://<host> segment")
    bootstrap_server = f"{match.group(1)}:9093"

    match = re.search(r"EntityPath=([^;]+)", connection_string)
    if not match:
        raise ValueError("SOURCE_CONNECTION_STRING is missing an EntityPath=<topic> segment")
    topic = match.group(1)

    return bootstrap_server, topic


def _forward_message(producer: EventHubProducerClient, msg) -> None:
    """Forward a Kafka message to EH-target, stamping spikeDetectedAtUtc from the
    Kafka broker's own message timestamp - the closest available proxy for "when
    the voltage spike filter produced this event", since Fabric appends to the
    derived stream's Kafka topic immediately after the filter match.
    """
    global forwarded_count
    payload = msg.value()

    try:
        parsed = json.loads(payload.decode("utf-8"))
        _, timestamp_ms = msg.timestamp()
        spike_detected_at = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        parsed["spikeDetectedAtUtc"] = spike_detected_at.isoformat()
        payload = json.dumps(parsed).encode("utf-8")
        summary = json.dumps(parsed)
    except Exception:
        summary = payload.decode("utf-8", errors="replace")

    batch = producer.create_batch()
    batch.add(EventData(payload))
    producer.send_batch(batch)
    forwarded_count += 1

    print(f"::notice::Forwarded event #{forwarded_count} to EH-target: {summary}")


def main() -> None:
    print(f"::notice::Current UTC time on runner: {datetime.now(timezone.utc).isoformat()}")

    source_connection_string = os.environ["SOURCE_CONNECTION_STRING"].strip().strip('"').strip("'")
    source_consumer_group = os.environ.get("SOURCE_CONSUMER_GROUP", "$Default")
    target_connection_string = os.environ["TARGET_CONNECTION_STRING"].strip().strip('"').strip("'")
    duration_seconds = int(os.environ.get("DURATION_SECONDS", "60"))
    auto_offset_reset = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")

    bootstrap_server, topic = _parse_source_connection_string(source_connection_string)
    print(f"::notice::Kafka bootstrap server: {bootstrap_server}, topic: {topic}")

    producer = EventHubProducerClient.from_connection_string(target_connection_string)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_server,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": source_connection_string,
            "group.id": source_consumer_group,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([topic])

    print(f"Consuming from Kafka topic '{topic}' for {duration_seconds}s, forwarding to EH-target...")
    deadline = time.monotonic() + duration_seconds
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"::notice::Kafka consumer error: {msg.error()}")
                continue
            _forward_message(producer, msg)
    except KafkaException as exc:
        print(f"::notice::Kafka consumer stopped: {exc}")
    finally:
        consumer.close()
        producer.close()

    print(f"Done. Forwarded {forwarded_count} event(s) to EH-target.")


if __name__ == "__main__":
    main()
