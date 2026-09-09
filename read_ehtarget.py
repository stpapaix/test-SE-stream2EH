"""Read JSON messages from Azure Event Hub EH-target.

Usage:
    python read_ehtarget.py

Auth: uses your local `az login` session (AzureCliCredential), falling back
to DefaultAzureCredential. Requires "Azure Event Hubs Data Receiver" role
on the EH-target entity (or namespace) for your account.
"""
"""Read JSON messages from Azure Event Hub EH-target.

Usage:
    python read_ehtarget.py [duration_seconds]

Auth: uses your local `az login` session (AzureCliCredential), falling back
to DefaultAzureCredential. Requires "Azure Event Hubs Data Receiver" role
on the EH-target entity (or namespace) for your account.

Behavior:
- Listens for duration_seconds (default 600s / 10 minutes if omitted), printing
  each message as it arrives.
- Remembers the last sequence number read per partition in a local checkpoint
  file (read_ehtarget_checkpoint.json), so messages already shown won't be
  printed again on the next run (Event Hubs itself can't delete messages,
  this just avoids re-reading old ones -> EH-target appears "empty" going
  forward).
"""
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone

from azure.eventhub import EventHubConsumerClient
from azure.identity import AzureCliCredential, ChainedTokenCredential, DefaultAzureCredential

NAMESPACE = "ehns-se-stream2eh-west-srzfa5vvnklsy.servicebus.windows.net"
EVENTHUB_NAME = "EH-target"
CONSUMER_GROUP = "$Default"
DEFAULT_DURATION_SECONDS = 600  # 10 minutes
if len(sys.argv) > 1:
    DURATION_SECONDS = int(sys.argv[1])
else:
    DURATION_SECONDS = DEFAULT_DURATION_SECONDS
    print(
        f"No duration argument provided, using default of {DEFAULT_DURATION_SECONDS}s "
        f"(usage: python read_ehtarget.py <duration_seconds>)"
    )
CHECKPOINT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "read_ehtarget_checkpoint.json")

credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential())
client = EventHubConsumerClient(
    fully_qualified_namespace=NAMESPACE,
    eventhub_name=EVENTHUB_NAME,
    consumer_group=CONSUMER_GROUP,
    credential=credential,
)

total = 0
last_seen = {}  # partition_id -> last sequence_number read this run
latencies = []  # seconds, one entry per reading where latency could be computed


def _load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_checkpoint(checkpoint: dict) -> None:
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f)


def _get_body_bytes(event) -> bytes:
    """Extract the raw body as bytes, working across azure-eventhub SDK versions.

    Some messages (e.g. Fabric connection-test probes) carry an empty/None
    body, which surfaces as a None chunk in the body generator/list.
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


def on_event_batch(partition_context, event_batch):
    global total
    for event in event_batch:
        body = _get_body_bytes(event).decode("utf-8")
        last_seen[partition_context.partition_id] = event.sequence_number
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            total += 1
            print(f"[{partition_context.partition_id}] (non-JSON) {body}", flush=True)
            continue

        # A message body may be a single reading (dict) or a micro-batch of
        # readings (list) - Fabric's Eventstream can batch multiple qualifying
        # events into one message.
        readings = parsed if isinstance(parsed, list) else [parsed]
        for reading in readings:
            total += 1
            if not isinstance(reading, dict):
                print(f"[{partition_context.partition_id}] (unexpected shape) {reading}", flush=True)
                continue
            device_id = reading.get("deviceId")
            voltage_v = reading.get("voltageV")
            latency_str = "n/a"
            spike_detected_at_str = reading.get("spikeDetectedAtUtc")
            if spike_detected_at_str and event.enqueued_time:
                spike_detected_at = datetime.fromisoformat(spike_detected_at_str)
                enqueued_at = event.enqueued_time
                if enqueued_at.tzinfo is None:
                    enqueued_at = enqueued_at.replace(tzinfo=timezone.utc)
                latency_seconds = (enqueued_at - spike_detected_at).total_seconds()
                latencies.append(latency_seconds)
                latency_str = f"{latency_seconds:.2f}s"
            print(
                f"[{partition_context.partition_id}] deviceId={device_id} voltageV={voltage_v} "
                f"latency(filter->EH-target)={latency_str}",
                flush=True,
            )


def stop_after_duration():
    time.sleep(DURATION_SECONDS)
    client.close()  # called from a separate thread -> safe, no deadlock


checkpoint = _load_checkpoint()
starting_position = {}
for pid, seq in checkpoint.items():
    starting_position[pid] = seq  # int sequence number -> resumes after it (exclusive by default)

threading.Thread(target=stop_after_duration, daemon=True).start()

with client:
    print(
        f"Listening on {EVENTHUB_NAME} for {DURATION_SECONDS}s "
        f"(resuming from checkpoint: {checkpoint or 'none, reading from beginning'})..."
    )
    try:
        client.receive_batch(
            on_event_batch=on_event_batch,
            starting_position=starting_position if starting_position else "-1",
            starting_position_inclusive=False,
            max_wait_time=5,
        )
    except Exception as exc:
        print(f"Stopped: {exc}")

# Merge newly-seen sequence numbers into the checkpoint so future runs skip them.
checkpoint.update(last_seen)
_save_checkpoint(checkpoint)

print(f"\nTotal messages read this run: {total}")
WARMUP_READINGS = 10
stable_latencies = latencies[WARMUP_READINGS:]
if stable_latencies:
    print(
        f"Average latency(filter->EH-target) this run: {sum(stable_latencies) / len(stable_latencies):.2f}s "
        f"over {len(stable_latencies)} reading(s) (first {WARMUP_READINGS} excluded as warmup)"
    )
else:
    print(
        f"Average latency(filter->EH-target) this run: n/a "
        f"(only {len(latencies)} readings with computable latency, need more than {WARMUP_READINGS} to exclude warmup)"
    )
print(f"Checkpoint saved to {CHECKPOINT_FILE}: {checkpoint}")


