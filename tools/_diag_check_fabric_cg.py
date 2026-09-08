"""Temporary diagnostic: confirm the 'fabric-eventstream-cg' consumer group (the one the
Eventstream source is configured to use) can currently read live events from EH-source.
This isolates whether the stall is on the Event Hub side or purely inside the Fabric
Eventstream engine/connection.
"""
import threading
import time
from datetime import datetime, timezone

from azure.eventhub import EventHubConsumerClient
from azure.identity import AzureCliCredential, ChainedTokenCredential, DefaultAzureCredential

NAMESPACE = "ehns-test-se-stream2eh-srzfa5vvnklsy.servicebus.windows.net"
EVENTHUB_NAME = "EH-source"
CONSUMER_GROUP = "fabric-eventstream-cg"
DURATION_SECONDS = 15

credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential())
client = EventHubConsumerClient(
    fully_qualified_namespace=NAMESPACE,
    eventhub_name=EVENTHUB_NAME,
    consumer_group=CONSUMER_GROUP,
    credential=credential,
)

total = 0


def on_event_batch(partition_context, event_batch):
    global total
    for event in event_batch:
        total += 1


def stop_after_duration():
    time.sleep(DURATION_SECONDS)
    client.close()


threading.Thread(target=stop_after_duration, daemon=True).start()

print(f"[{datetime.now(timezone.utc).isoformat()}] Listening on consumer group '{CONSUMER_GROUP}' for {DURATION_SECONDS}s...")
with client:
    try:
        client.receive_batch(on_event_batch=on_event_batch, starting_position="@latest", max_wait_time=5)
    except Exception as exc:
        print(f"Stopped: {exc}")

print(f"Total messages seen on '{CONSUMER_GROUP}' in {DURATION_SECONDS}s: {total}")
