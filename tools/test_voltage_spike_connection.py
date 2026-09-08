"""Standalone test: verify the voltage-spike-endpoint connection string authenticates
for a REAL partition-level receive (not just metadata/get_eventhub_properties, which
always succeeds even when the actual data-plane receive link fails).

Run locally:
  $env:VOLTAGE_SPIKE_CONNECTION_STRING = "paste the connection string here (not shown to anyone else)"
  python tools/test_voltage_spike_connection.py

This does NOT print or log the connection string itself - only success/failure.
"""
import os
import sys
import threading
import time

print("Script starting...", flush=True)

from azure.eventhub import EventHubConsumerClient

print("Import done, reading env var...", flush=True)

connection_string = os.environ.get("VOLTAGE_SPIKE_CONNECTION_STRING")
if not connection_string:
    print("ERROR: Set VOLTAGE_SPIKE_CONNECTION_STRING env var first (see docstring).", flush=True)
    sys.exit(1)

connection_string = connection_string.strip().strip('"').strip("'")
print(f"Connection string found (length={len(connection_string)} chars).", flush=True)

try:
    probe_client = EventHubConsumerClient.from_connection_string(connection_string, consumer_group="$Default")
    with probe_client:
        props = probe_client.get_eventhub_properties()
    partition_ids = props["partition_ids"]
    print(f"SUCCESS (metadata): partitions={partition_ids}", flush=True)
except Exception as exc:
    print(f"FAILED (metadata): {type(exc).__name__}: {exc}", flush=True)
    sys.exit(1)

print("Now attempting a REAL partition-level receive (this is the part that fails in CI)...", flush=True)

received = {"count": 0}
error_holder = {}


def on_event(partition_context, event):
    if event is not None:
        received["count"] += 1


try:
    receive_client = EventHubConsumerClient.from_connection_string(connection_string, consumer_group="$Default")

    def close_after():
        time.sleep(10)
        receive_client.close()

    threading.Thread(target=close_after, daemon=True).start()

    with receive_client:
        receive_client.receive(
            on_event=on_event,
            partition_id=partition_ids[0],
            starting_position="-1",
            max_wait_time=3,
        )
    print(f"SUCCESS (partition receive): received {received['count']} event(s) (0 is fine - auth worked).", flush=True)
except Exception as exc:
    print(f"FAILED (partition receive): {type(exc).__name__}: {exc}", flush=True)
    sys.exit(1)

print("Done.", flush=True)
