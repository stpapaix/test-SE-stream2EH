"""Standalone test: verify the voltage-spike-endpoint connection string authenticates,
independent of GitHub Actions/secrets. Fails/succeeds within a few seconds - does not
wait for actual messages.

Run locally:
  $env:VOLTAGE_SPIKE_CONNECTION_STRING = "paste the connection string here (not shown to anyone else)"
  python tools/test_voltage_spike_connection.py

This does NOT print or log the connection string itself - only success/failure.
"""
import os
import sys

print("Script starting...", flush=True)

from azure.eventhub import EventHubConsumerClient

print("Import done, reading env var...", flush=True)

connection_string = os.environ.get("VOLTAGE_SPIKE_CONNECTION_STRING")
if not connection_string:
    print("ERROR: Set VOLTAGE_SPIKE_CONNECTION_STRING env var first (see docstring).", flush=True)
    sys.exit(1)

print(f"Connection string found (length={len(connection_string)} chars). Connecting...", flush=True)

try:
    client = EventHubConsumerClient.from_connection_string(
        connection_string, consumer_group="$Default"
    )
    print("Client created. Requesting eventhub properties (fails fast on bad auth)...", flush=True)
    props = client.get_eventhub_properties()
    print(f"SUCCESS: authenticated. Eventhub name={props.get('eventhub_name')}, partitions={props.get('partition_ids')}", flush=True)
    client.close()
except Exception as exc:
    print(f"FAILED: {type(exc).__name__}: {exc}", flush=True)
    sys.exit(1)

print("Done.", flush=True)
