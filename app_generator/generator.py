"""App_generator: simulates energy-management telemetry and streams it directly to a
Fabric Eventstream Custom Endpoint source (Event Hubs protocol).

Usage (env vars):
  EVENTHUB_CONNECTION_STRING   connection string for the Fabric custom endpoint source
                                (Event Hub protocol, includes EntityPath)
  DURATION_SECONDS              how long to run (default: 300)
  INTERVAL_SECONDS              seconds between batches (default: 5)
  BATCH_SIZE                    events per batch (default: 10)

Voltage: 20% of readings simulate a spike in the 240-245V range; the rest are 228-232V.

Auth: SAS connection string only - no Azure AD / azure/login needed, since the
Fabric custom endpoint manages its own keys independent of any Event Hub namespace.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from azure.eventhub import EventData, EventHubProducerClient

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
        "voltageV": round(random.uniform(240, 245) if random.random() < 0.20 else random.uniform(228, 232), 1),
        "currentA": round(abs(active_power) * 1000 / 230, 1),
        "frequencyHz": round(random.uniform(49.95, 50.05), 3),
        "powerFactor": round(random.uniform(0.9, 1.0), 2),
        "cumulativeEnergyKwh": device["cumulativeEnergyKwh"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    connection_string = os.environ["EVENTHUB_CONNECTION_STRING"]
    duration_seconds = int(os.environ.get("DURATION_SECONDS", "300"))
    interval_seconds = float(os.environ.get("INTERVAL_SECONDS", "5"))
    batch_size = int(os.environ.get("BATCH_SIZE", "10"))

    producer = EventHubProducerClient.from_connection_string(connection_string)

    print(f"Streaming to Fabric custom endpoint source for {duration_seconds}s "
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
