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
import os
import threading
import time
from datetime import datetime, timedelta, timezone

from azure.eventhub import EventData, EventHubConsumerClient, EventHubProducerClient

forwarded_count = 0


def _on_event(producer: EventHubProducerClient, partition_context, event):
    global forwarded_count
    raw = b"".join(event.body_as_bytes())
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
    source_connection_string = os.environ["SOURCE_CONNECTION_STRING"]
    source_consumer_group = os.environ.get("SOURCE_CONSUMER_GROUP", "$Default")
    target_connection_string = os.environ["TARGET_CONNECTION_STRING"]
    duration_seconds = int(os.environ.get("DURATION_SECONDS", "60"))
    lookback_minutes = int(os.environ.get("LOOKBACK_MINUTES", "20"))
    starting_position = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)

    producer = EventHubProducerClient.from_connection_string(target_connection_string)
    consumer = EventHubConsumerClient.from_connection_string(
        source_connection_string, consumer_group=source_consumer_group
    )

    print(f"Consuming from custom endpoint for {duration_seconds}s (lookback {lookback_minutes}m), forwarding to EH-target...")

    def stop_after_duration():
        time.sleep(duration_seconds)
        print("Duration elapsed, closing consumer...")
        consumer.close()

    stopper = threading.Thread(target=stop_after_duration, daemon=True)
    stopper.start()

    try:
        consumer.receive(
            on_event=lambda partition_context, event: _on_event(producer, partition_context, event),
            starting_position=starting_position,
        )
    except Exception as exc:  # noqa: BLE001 - consumer.close() during receive raises, that's expected
        print(f"Consumer stopped: {exc}")
    finally:
        producer.close()

    print(f"Done. Forwarded {forwarded_count} event(s) to EH-target.")


if __name__ == "__main__":
    main()
