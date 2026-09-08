"""forwarder: consumes voltage-spike events from a Fabric Eventstream custom endpoint
and republishes them to the Azure Event Hub EH-target.

Usage (env vars):
  SOURCE_CONNECTION_STRING   connection string for the Fabric custom endpoint (Event Hub protocol)
  SOURCE_CONSUMER_GROUP      consumer group on the custom endpoint (default: $Default)
  TARGET_CONNECTION_STRING   connection string for EH-target (fabric-send-policy)
  DURATION_SECONDS           how long to consume/forward per run (default: 60)
  LOOKBACK_MINUTES           how far back to look for events not yet seen (default: 20)
"""
import json
import hashlib
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from azure.eventhub import EventData, EventHubConsumerClient, EventHubProducerClient

forwarded_count = 0


def _get_body_bytes(event) -> bytes:
    """Extract the raw body as bytes, working across azure-eventhub SDK versions.

    Some messages (e.g. connection-test probes) carry an empty/None body,
    which surfaces as a None chunk in the body generator/list.
    """
    if hasattr(event, "body_as_bytes"):
        raw = event.body_as_bytes()
    else:
        raw = event.body
    if raw is None:
        return b""
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    return b"".join(chunk for chunk in raw if chunk is not None)


def _on_event(producer: EventHubProducerClient, partition_context, event):
    global forwarded_count
    raw = _get_body_bytes(event)
    batch = producer.create_batch()
    batch.add(EventData(raw))
    producer.send_batch(batch)
    forwarded_count += 1

    try:
        payload = json.loads(raw.decode("utf-8"))
        summary = json.dumps(payload)
    except Exception:
        summary = raw.decode("utf-8", errors="replace")

    print(
        f"::notice::Forwarded event #{forwarded_count} to EH-target "
        f"(partition {partition_context.partition_id}, seq {event.sequence_number}): {summary}"
    )
    partition_context.update_checkpoint(event)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("azure.eventhub").setLevel(logging.DEBUG)
    logging.getLogger("uamqp").setLevel(logging.DEBUG)

    print(f"::notice::Current UTC time on runner: {datetime.now(timezone.utc).isoformat()}")

    source_connection_string = os.environ["SOURCE_CONNECTION_STRING"].strip().strip('"').strip("'")
    source_consumer_group = os.environ.get("SOURCE_CONSUMER_GROUP", "$Default")
    target_connection_string = os.environ["TARGET_CONNECTION_STRING"].strip().strip('"').strip("'")
    duration_seconds = int(os.environ.get("DURATION_SECONDS", "60"))
    lookback_minutes = int(os.environ.get("LOOKBACK_MINUTES", "20"))
    starting_position = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)

    source_hash = hashlib.sha256(source_connection_string.encode("utf-8")).hexdigest()[:12]
    print(
        f"::notice::SOURCE_CONNECTION_STRING: length={len(source_connection_string)} sha256[:12]={source_hash}"
    )

    producer = EventHubProducerClient.from_connection_string(target_connection_string)
    probe_consumer = EventHubConsumerClient.from_connection_string(
        source_connection_string, consumer_group=source_consumer_group
    )
    with probe_consumer:
        partition_ids = probe_consumer.get_partition_ids()
    print(f"::notice::Discovered partitions: {partition_ids}")

    print(f"Consuming from custom endpoint for {duration_seconds}s (lookback {lookback_minutes}m), forwarding to EH-target...")

    # Fabric's custom endpoint rejects CBS auth when multiple partition receiver
    # links authenticate concurrently, so poll partitions sequentially - one
    # connection/link open at a time - instead of all-at-once (default EventProcessor
    # behavior, and even a thread-per-partition approach, both open links in parallel).
    per_partition_seconds = 5
    deadline = time.monotonic() + duration_seconds
    round_num = 0
    while time.monotonic() < deadline:
        round_num += 1
        for partition_id in partition_ids:
            if time.monotonic() >= deadline:
                break
            partition_consumer = EventHubConsumerClient.from_connection_string(
                source_connection_string, consumer_group=source_consumer_group
            )

            def close_after(client=partition_consumer):
                time.sleep(per_partition_seconds)
                client.close()

            closer = threading.Thread(target=close_after, daemon=True)
            closer.start()
            try:
                with partition_consumer:
                    partition_consumer.receive(
                        on_event=lambda partition_context, event: _on_event(producer, partition_context, event),
                        partition_id=partition_id,
                        starting_position=starting_position,
                        max_wait_time=3,
                    )
            except Exception as exc:  # noqa: BLE001 - close() during receive raises, that's expected
                print(f"::notice::[round {round_num}] Partition {partition_id} consumer stopped: {exc}")

    producer.close()

    print(f"Done. Forwarded {forwarded_count} event(s) to EH-target.")


if __name__ == "__main__":
    main()
