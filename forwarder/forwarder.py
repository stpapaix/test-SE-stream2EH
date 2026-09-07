"""forwarder: consumes voltage-spike events from a Fabric Eventstream custom endpoint
and republishes them to the Azure Event Hub EH-target.

Usage (env vars):
  SOURCE_CONNECTION_STRING   connection string for the Fabric custom endpoint (Event Hub protocol)
  SOURCE_CONSUMER_GROUP      consumer group on the custom endpoint (default: $Default)
  TARGET_CONNECTION_STRING   connection string for EH-target (fabric-send-policy)
  DURATION_SECONDS           how long to consume/forward per run (default: 60)
"""
import os
import threading
import time

from azure.eventhub import EventData, EventHubConsumerClient, EventHubProducerClient

forwarded_count = 0


def _on_event(producer: EventHubProducerClient, partition_context, event):
    global forwarded_count
    batch = producer.create_batch()
    batch.add(EventData(event.body_as_bytes()))
    producer.send_batch(batch)
    forwarded_count += 1
    print(f"Forwarded event #{forwarded_count} (partition {partition_context.partition_id})")
    partition_context.update_checkpoint(event)


def main() -> None:
    source_connection_string = os.environ["SOURCE_CONNECTION_STRING"]
    source_consumer_group = os.environ.get("SOURCE_CONSUMER_GROUP", "$Default")
    target_connection_string = os.environ["TARGET_CONNECTION_STRING"]
    duration_seconds = int(os.environ.get("DURATION_SECONDS", "60"))

    producer = EventHubProducerClient.from_connection_string(target_connection_string)
    consumer = EventHubConsumerClient.from_connection_string(
        source_connection_string, consumer_group=source_consumer_group
    )

    print(f"Consuming from custom endpoint for {duration_seconds}s, forwarding to EH-target...")

    def stop_after_duration():
        time.sleep(duration_seconds)
        print("Duration elapsed, closing consumer...")
        consumer.close()

    stopper = threading.Thread(target=stop_after_duration, daemon=True)
    stopper.start()

    try:
        consumer.receive(
            on_event=lambda partition_context, event: _on_event(producer, partition_context, event),
            starting_position="-1",  # only new events from now on
        )
    except Exception as exc:  # noqa: BLE001 - consumer.close() during receive raises, that's expected
        print(f"Consumer stopped: {exc}")
    finally:
        producer.close()

    print(f"Done. Forwarded {forwarded_count} event(s) to EH-target.")


if __name__ == "__main__":
    main()
