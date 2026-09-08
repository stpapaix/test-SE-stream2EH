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
  TARGET_CONNECTION_STRING   connection string for EH-target, used only to derive the
                             namespace FQDN and entity name (its SharedAccessKey is not used
                             for auth: EH-target enforces disableLocalAuth via Azure Policy,
                             so the producer authenticates with Azure AD/DefaultAzureCredential
                             instead - the caller's identity must have "Azure Event Hubs Data
                             Sender" on eh-target: Managed Identity in Container Apps, or an
                             az-cli-logged-in principal in GitHub Actions via azure/login).
  DURATION_SECONDS           how long to consume/forward per run (default: 60)
  KAFKA_AUTO_OFFSET_RESET    "earliest" or "latest" (default: earliest)
"""
import json
import os
import re
import time
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import DefaultAzureCredential
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


def _parse_target_connection_string(connection_string: str) -> tuple[str, str]:
    """Extract (fully_qualified_namespace, eventhub_name) from an Event-Hub connection string."""
    match = re.search(r"Endpoint=sb://([^/;]+)", connection_string)
    if not match:
        raise ValueError("TARGET_CONNECTION_STRING is missing an Endpoint=sb://<host> segment")
    fully_qualified_namespace = match.group(1)

    match = re.search(r"EntityPath=([^;]+)", connection_string)
    if not match:
        raise ValueError("TARGET_CONNECTION_STRING is missing an EntityPath=<eventhub> segment")
    eventhub_name = match.group(1)

    return fully_qualified_namespace, eventhub_name


def _forward_message(producer: EventHubProducerClient, msg) -> None:
    """Forward a Kafka message to EH-target, stamping spikeDetectedAtUtc from the
    Kafka broker's own message timestamp - the closest available proxy for "when
    the voltage spike filter produced this event", since Fabric appends to the
    derived stream's Kafka topic immediately after the filter match.

    A single Kafka message may contain either one reading (a JSON object) or a
    micro-batch of readings (a JSON array of objects) - Fabric's Eventstream can
    batch multiple qualifying events together. Each reading is forwarded as its
    own EventData so EH-target always receives one JSON object per message.
    """
    global forwarded_count
    raw_payload = msg.value()
    _, timestamp_ms = msg.timestamp()
    spike_detected_at = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)

    try:
        parsed = json.loads(raw_payload.decode("utf-8"))
    except Exception:
        parsed = None

    if isinstance(parsed, list):
        readings = parsed
    elif isinstance(parsed, dict):
        readings = [parsed]
    else:
        readings = None

    if readings is None:
        batch = producer.create_batch()
        batch.add(EventData(raw_payload))
        producer.send_batch(batch)
        forwarded_count += 1
        print(f"::notice::Forwarded event #{forwarded_count} to EH-target (unparsed): {raw_payload!r}")
        return

    batch = producer.create_batch()
    for reading in readings:
        if isinstance(reading, dict):
            reading["spikeDetectedAtUtc"] = spike_detected_at.isoformat()
        batch.add(EventData(json.dumps(reading)))
    producer.send_batch(batch)
    forwarded_count += len(readings)

    print(f"::notice::Forwarded {len(readings)} event(s) (total {forwarded_count}) to EH-target: {json.dumps(readings)}")


def main() -> None:
    print(f"::notice::Current UTC time on runner: {datetime.now(timezone.utc).isoformat()}")

    source_connection_string = os.environ["SOURCE_CONNECTION_STRING"].strip().strip('"').strip("'")
    source_consumer_group = os.environ.get("SOURCE_CONSUMER_GROUP", "$Default")
    target_connection_string = os.environ["TARGET_CONNECTION_STRING"].strip().strip('"').strip("'")
    duration_seconds = int(os.environ.get("DURATION_SECONDS", "0"))
    auto_offset_reset = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")

    bootstrap_server, topic = _parse_source_connection_string(source_connection_string)
    print(f"::notice::Kafka bootstrap server: {bootstrap_server}, topic: {topic}")

    target_namespace, target_eventhub = _parse_target_connection_string(target_connection_string)
    print(f"::notice::EH-target namespace: {target_namespace}, eventhub: {target_eventhub} (Azure AD auth)")
    producer = EventHubProducerClient(
        fully_qualified_namespace=target_namespace,
        eventhub_name=target_eventhub,
        credential=DefaultAzureCredential(),
    )

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

    # DURATION_SECONDS <= 0 (or unset) means run forever - used for the Container Apps
    # always-on deployment. GitHub Actions always sets DURATION_SECONDS explicitly.
    deadline = time.monotonic() + duration_seconds if duration_seconds > 0 else None
    if deadline is None:
        print(f"Consuming from Kafka topic '{topic}' continuously, forwarding to EH-target...")
    else:
        print(f"Consuming from Kafka topic '{topic}' for {duration_seconds}s, forwarding to EH-target...")

    try:
        last_heartbeat = time.monotonic()
        while deadline is None or time.monotonic() < deadline:
            try:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    if time.monotonic() - last_heartbeat >= 30:
                        assignment = consumer.assignment()
                        print(
                            f"::notice::Heartbeat: no message in the last poll window. "
                            f"Partitions assigned: {[p.partition for p in assignment] if assignment else 'none yet'}"
                        )
                        last_heartbeat = time.monotonic()
                    continue
                last_heartbeat = time.monotonic()
                if msg.error():
                    print(f"::notice::Kafka consumer error: {msg.error()}")
                    continue
                print(
                    f"::notice::Captured event: partition={msg.partition()} offset={msg.offset()} "
                    f"key={msg.key()} value_size={len(msg.value()) if msg.value() else 0} bytes"
                )
                _forward_message(producer, msg)
            except KafkaException as exc:
                print(f"::notice::Kafka consumer error, continuing: {exc}")
            except Exception as exc:  # noqa: BLE001 - keep the long-running loop alive on transient errors
                print(f"::notice::Unexpected error forwarding message, continuing: {exc}")
    finally:
        consumer.close()
        producer.close()

    print(f"Done. Forwarded {forwarded_count} event(s) to EH-target.")


if __name__ == "__main__":
    main()
