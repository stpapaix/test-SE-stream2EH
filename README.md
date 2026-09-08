# test-SE-stream2EH
Test RTI Streaming to external Azure Event Hub

## Demo: Energy Telemetry Streaming (Fabric Eventstream → Eventhouse + filtered Azure Event Hub sink)

End-to-end real-time energy telemetry pipeline. A generator publishes simulated device
readings into a Fabric Eventstream via a Custom Endpoint (SAS-based, no Azure AD
dependency). Fabric fans the stream out to an Eventhouse for analytics and, in parallel,
filters for voltage spikes and republishes those onto a second Custom Endpoint. A
forwarder job (Kafka protocol) picks up the filtered spikes and republishes them onto a
plain Azure Event Hub (`EH-target`) for external/legacy consumers. Everything runs on a
schedule via GitHub Actions.

### Architecture

```mermaid
flowchart TB
    subgraph GH["GitHub Actions"]
        GEN["demoehab.yml (every 30 min)<br/>runs app_generator/generator.py"]
        FWD["spike-forwarder.yml (every 15 min)<br/>runs forwarder/forwarder.py"]
    end

    subgraph FAB["Microsoft Fabric workspace: test-SE-stream2EH"]
        direction TB
        SRC["Custom Endpoint SOURCE<br/>generator-source<br/>(SAS, Event Hub protocol)"]
        ES["Eventstream: stream4ehab"]
        STREAM["DefaultStream"]
        FILTER["Filter operator<br/>voltage_spike_filter<br/>voltageV &gt;= 240"]
        DSTREAM["DerivedStream"]
        EH_HOUSE[("Eventhouse: db4ehab<br/>table: EnergyTelemetry")]
        DEST["Custom Endpoint DESTINATION<br/>voltage-spike-endpoint<br/>(SAS, Kafka + Event Hub protocols)"]

        SRC --> ES --> STREAM
        STREAM --> EH_HOUSE
        STREAM --> FILTER --> DSTREAM --> DEST
    end

    subgraph AZ["Azure Resource Group: test-SE-stream2EH"]
        EHT[("Azure Event Hub<br/>EH-target<br/>namespace: ehns-test-se-stream2eh-*<br/>local/SAS auth enabled")]
    end

    GEN -- "azure-eventhub SDK<br/>(AMQP, SAS)" --> SRC
    DEST -- "Kafka protocol<br/>SASL_SSL / PLAIN" --> FWD
    FWD -- "azure-eventhub SDK<br/>(AMQP, SAS send policy)" --> EHT
```

### Why two different protocols for the forwarder?

`voltage-spike-endpoint` is a Fabric-managed Custom Endpoint destination. Its SAS
connection string authenticates fine for AMQP **metadata** operations (e.g.
`get_eventhub_properties`), but Fabric consistently rejects the AMQP **CBS auth**
required for an actual partition-level receive on that endpoint
(`com.microsoft:auth-failed`) — confirmed even with fully sequential, one-connection-
at-a-time testing. Switching the forwarder's source consumer to the **Kafka protocol**
(SASL_SSL/PLAIN, same underlying connection string, no new secret needed) works
around this, since it authenticates via a completely different code path. The
producer side (into the plain Azure Event Hub `EH-target`) is unaffected and still
uses the standard `azure-eventhub` AMQP SDK.

### Components

| Component | Location | Runs via |
|---|---|---|
| Generator (simulates energy telemetry, publishes to `generator-source`) | [app_generator/generator.py](app_generator/generator.py) | [.github/workflows/demoehab.yml](.github/workflows/demoehab.yml) — every 30 min or manual dispatch |
| Forwarder (Kafka-consumes voltage spikes, republishes to `EH-target`) | [forwarder/forwarder.py](forwarder/forwarder.py) | [.github/workflows/spike-forwarder.yml](.github/workflows/spike-forwarder.yml) — every 15 min or manual dispatch |
| Azure Event Hub namespace + `EH-target` entity + `fabric-send-policy` | [infra/eventhub.bicep](infra/eventhub.bicep) | [.github/workflows/deploy-infra.yml](.github/workflows/deploy-infra.yml) |
| Fabric Eventstream `stream4ehab` + Eventhouse `db4ehab` topology (source, filter, destinations) | Fabric workspace `test-SE-stream2EH` (wired via Fabric REST API) | One-time setup; see `fabric-eventstream-wire*.yml` (historical/reference) |
| Local read-only verifier for `EH-target` | [read_ehtarget.py](read_ehtarget.py) | Run manually (`az login` + Data Receiver role) |
| Local connection-string diagnostic | [tools/test_voltage_spike_connection.py](tools/test_voltage_spike_connection.py) | Run manually, connection string via env var only |

### Simulated data (generator)

One JSON event per reading, fleet of `smart_meter`, `solar_inverter`, `wind_turbine`,
`battery_storage`, and `ev_charger` devices:

```json
{
  "eventId": "…",
  "deviceId": "solar_inverter-0003",
  "deviceType": "solar_inverter",
  "siteId": "substation-07",
  "region": "west-eu",
  "activePowerKw": 4.82,
  "voltageV": 230.1,
  "currentA": 20.9,
  "frequencyHz": 50.01,
  "powerFactor": 0.97,
  "cumulativeEnergyKwh": 12345.678,
  "timestamp": "2026-09-07T12:00:00+00:00"
}
```

`voltageV` spikes into the 240-245V range with 20% probability per reading — this is what the
Eventstream's `voltage_spike_filter` (`voltageV >= 240`) selects for the derived stream
and, ultimately, for `EH-target`.

### GitHub secrets required

| Secret | Used by | Purpose |
|---|---|---|
| `GENERATOR_SOURCE_CONNECTION_STRING` | `demoehab.yml` | SAS connection string for the `generator-source` Custom Endpoint (Event Hub protocol) |
| `FABRIC_ENDPOINT_CONNECTION_STRING` | `spike-forwarder.yml` | SAS connection string for `voltage-spike-endpoint` (bootstrap server + topic are parsed from it for Kafka) |
| `EHTARGET_SEND_CONNECTION_STRING` | `spike-forwarder.yml` | SAS connection string for `EH-target`'s `fabric-send-policy` (Send rights) |

All three connection strings/keys are only obtainable from the Fabric/Azure portal UI
(no public API for Custom Endpoint keys) and must be rotated via the portal if ever
exposed. **Never paste live connection strings into chat or commit them to the repo.**

### Azure prerequisites

- Event Hubs namespace `disableLocalAuth` must be `false` — the forwarder and generator
  both authenticate via SAS connection strings, not Azure AD/workspace identity.

