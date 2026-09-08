"""Quick diagnostic: read a short window of messages from EH-source to confirm the
generator is actually publishing events. Uses local az login (AzureCliCredential).
"""
import json
import threading
import time
from datetime import datetime, timedelta, timezone

from azure.eventhub import EventHubConsumerClient
from azure.identity import AzureCliCredential, ChainedTokenCredential, DefaultAzureCredential

NAMESPACE = "ehns-test-se-stream2eh-srzfa5vvnklsy.servicebus.windows.net"
EVENTHUB_NAME = "EH-source"
DURATION_SECONDS = 20
LOOKBACK_MINUTES = 20

credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential())
client = EventHubConsumerClient(
    fully_qualified_namespace=NAMESPACE,
    eventhub_name=EVENTHUB_NAME,
    consumer_group="$Default",
    credential=credential,
)

total = 0


def _get_body_bytes(event) -> bytes:
    if hasattr(event, "body_as_bytes"):
        raw = event.body_as_bytes()
    else:
        raw = event.body
    if raw is None:
        return b""
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw)
    return b"".join(chunk for chunk in raw if chunk is not None)


def on_event_batch(partition_context, event_batch):
    global total
    for event in event_batch:
        total += 1
        body = _get_body_bytes(event).decode("utf-8")
        if total <= 3:
            print(f"[{partition_context.partition_id}] {body[:200]}", flush=True)


def stop_after_duration():
    time.sleep(DURATION_SECONDS)
    client.close()


threading.Thread(target=stop_after_duration, daemon=True).start()

with client:
    print(f"Listening on {EVENTHUB_NAME} (lookback {LOOKBACK_MINUTES}m) for {DURATION_SECONDS}s...")
    try:
        client.receive_batch(
            on_event_batch=on_event_batch,
            starting_position=datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES),
            max_wait_time=5,
        )
    except Exception as exc:
        print(f"Stopped: {exc}")

print(f"\nTotal messages seen in {DURATION_SECONDS}s: {total}")
