"""App_generator: simulates energy-management telemetry and streams it to Azure Event Hubs.

Usage (env vars):
  EVENTHUB_NAMESPACE   fully-qualified namespace, e.g. ehns-xxx.servicebus.windows.net
  EVENTHUB_NAME        event hub entity name (default: EH-source)
  DURATION_SECONDS     how long to run (default: 300)
  INTERVAL_SECONDS     seconds between batches (default: 5)
  BATCH_SIZE           events per batch (default: 10)

Auth: uses AzureCliCredential first (works right after `azure/login` in GitHub Actions
or after a local `az login`), falling back to DefaultAzureCredential.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import AzureCliCredential, ChainedTokenCredential, DefaultAzureCredential

DEVICE_TYPES = ["smart_meter", "solar_inverter", "wind_turbine", "battery_storage", "ev_charger"]
REGIONS = ["west-eu", "north-eu", "south-eu"]

FLEET = [
    {
        "deviceId": f"{dtype}-{i:04d}",
        "deviceType": dtype,
        "siteId": f"substation-{random.randint(1, 12):02d}",
        "region": random.choice(REGIONS),
        "cumulativeEnergyKwh": round(random.uniform(1000, 50000), 2),
    }
    for dtype in DEVICE_TYPES
    for i in range(1, 6)
]


def _simulate_reading(device: dict) -> dict:
    dtype = device["deviceType"]
    hour = datetime.now(timezone.utc).hour

    if dtype == "solar_inverter":
        # No output at night, peak around midday
        daylight = max(0.0, 1 - abs(hour - 12) / 6)
        active_power = round(random.uniform(0, 5) * daylight, 2)
    elif dtype == "wind_turbine":
        active_power = round(random.uniform(0, 3), 2)
    elif dtype == "ev_charger":
        active_power = round(random.choice([0, 0, 7.4, 11, 22]) + random.uniform(-0.3, 0.3), 2)
    elif dtype == "battery_storage":
        active_power = round(random.uniform(-5, 5), 2)  # negative = charging
    else:  # smart_meter
        active_power = round(random.uniform(0.2, 6), 2)

    device["cumulativeEnergyKwh"] = round(device["cumulativeEnergyKwh"] + max(active_power, 0) / 720, 3)

    return {
        "eventId": str(uuid.uuid4()),
        "deviceId": device["deviceId"],
        "deviceType": dtype,
        "siteId": device["siteId"],
        "region": device["region"],
        "activePowerKw": active_power,
        "voltageV": round(random.uniform(228, 232), 1),
        "currentA": round(abs(active_power) * 1000 / 230, 1),
        "frequencyHz": round(random.uniform(49.95, 50.05), 3),
        "powerFactor": round(random.uniform(0.9, 1.0), 2),
        "cumulativeEnergyKwh": device["cumulativeEnergyKwh"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    namespace = os.environ["EVENTHUB_NAMESPACE"]
    eventhub_name = os.environ.get("EVENTHUB_NAME", "EH-source")
    duration_seconds = int(os.environ.get("DURATION_SECONDS", "300"))
    interval_seconds = float(os.environ.get("INTERVAL_SECONDS", "5"))
    batch_size = int(os.environ.get("BATCH_SIZE", "10"))

    credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential())
    producer = EventHubProducerClient(
        fully_qualified_namespace=namespace,
        eventhub_name=eventhub_name,
        credential=credential,
    )

    print(f"Streaming to {namespace}/{eventhub_name} for {duration_seconds}s "
          f"({batch_size} events every {interval_seconds}s)")

    start = time.monotonic()
    sent_total = 0
    with producer:
        while time.monotonic() - start < duration_seconds:
            batch = producer.create_batch()
            devices = random.sample(FLEET, k=min(batch_size, len(FLEET)))
            for device in devices:
                reading = _simulate_reading(device)
                try:
                    batch.add(EventData(json.dumps(reading)))
                except ValueError:
                    producer.send_batch(batch)
                    batch = producer.create_batch()
                    batch.add(EventData(json.dumps(reading)))
            producer.send_batch(batch)
            sent_total += len(devices)
            print(f"Sent batch of {len(devices)} events (total {sent_total})")
            time.sleep(interval_seconds)

    print(f"Done. Sent {sent_total} events total.")


if __name__ == "__main__":
    main()
