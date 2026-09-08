# test-SE-stream2EH
Test RTI Streaming to external Azure Event Hub

## Demo: Energy Telemetry Streaming (Fabric Eventstream → Eventhouse + filtered Azure Event Hub sink)

End-to-end real-time energy telemetry pipeline. A generator publishes simulated device
readings into a Fabric Eventstream via a Custom Endpoint (SAS-based, no Azure AD
dependency). Fabric fans the stream out to an Eventhouse for analytics and, in parallel,
filters for voltage spikes and republishes those onto a second Custom Endpoint. An
always-on forwarder running in **Azure Container Apps** (Kafka protocol) picks up the
filtered spikes and republishes them onto a plain Azure Event Hub (`EH-target`) for
external/legacy consumers, authenticating with its **managed identity** (Azure AD)
rather than a SAS key. The generator runs on a schedule via GitHub Actions.

### Architecture

```mermaid
flowchart TB
    subgraph GH["GitHub Actions"]
        GEN["demoehab.yml (every 30 min)<br/>runs app_generator/generator.py"]
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

    subgraph ACA["Azure Container Apps: cae-se-stream2eh"]
        CA["ca-spike-forwarder<br/>(always-on, forwarder/forwarder.py)<br/>system-assigned managed identity"]
    end

    subgraph AZ["Azure Resource Group: test-SE-stream2EH"]
        EHT[("Azure Event Hub<br/>EH-target<br/>namespace: ehns-test-se-stream2eh-*<br/>disableLocalAuth=true (policy-enforced)")]
    end

    GEN -- "azure-eventhub SDK<br/>(AMQP, SAS)" --> SRC
    DEST -- "Kafka protocol<br/>SASL_SSL / PLAIN (SAS)" --> CA
    CA -- "azure-eventhub SDK<br/>(AMQP, Azure AD/<br/>managed identity)" --> EHT
```

### Why two different protocols/auth modes for the forwarder?

`voltage-spike-endpoint` is a Fabric-managed Custom Endpoint destination. Its SAS
connection string authenticates fine for AMQP **metadata** operations (e.g.
`get_eventhub_properties`), but Fabric consistently rejects the AMQP **CBS auth**
required for an actual partition-level receive on that endpoint
(`com.microsoft:auth-failed`) — confirmed even with fully sequential, one-connection-
at-a-time testing. Switching the forwarder's source consumer to the **Kafka protocol**
(SASL_SSL/PLAIN, same underlying connection string, no new secret needed) works
around this, since it authenticates via a completely different code path.

On the producer side, `EH-target`'s namespace has an Azure Policy
(`EventHub_DisableLocalAuth_Modify`) that enforces `disableLocalAuth=true` and silently
reverts any attempt to disable it — so **no SAS-based sender can ever authenticate**
against it (`CBS Token authentication failed`). The forwarder's producer therefore uses
`DefaultAzureCredential` against the Container App's system-assigned managed identity,
which has been granted the **Azure Event Hubs Data Sender** role on `eh-target`.

### Components

| Component | Location | Runs via |
|---|---|---|
| Generator (simulates energy telemetry, publishes to `generator-source`) | [app_generator/generator.py](app_generator/generator.py) | [.github/workflows/demoehab.yml](.github/workflows/demoehab.yml) — every 30 min or manual dispatch |
| Forwarder (Kafka-consumes voltage spikes, republishes to `EH-target` via managed identity) | [forwarder/forwarder.py](forwarder/forwarder.py) | Always-on Azure Container App `ca-spike-forwarder`, defined in [infra/containerapp-forwarder.bicep](infra/containerapp-forwarder.bicep) |
| Deploy/redeploy forwarder infra (ACR, environment, Container App, managed identity) | | [.github/workflows/deploy-container-forwarder.yml](.github/workflows/deploy-container-forwarder.yml) — manual dispatch, use after infra changes |
| Rebuild/redeploy forwarder image only (code-only changes) | | [.github/workflows/update-container-forwarder.yml](.github/workflows/update-container-forwarder.yml) — manual dispatch, faster path, skips Bicep |
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

### GitHub secrets/variables required

| Secret/Variable | Used by | Purpose |
|---|---|---|
| `GENERATOR_SOURCE_CONNECTION_STRING` | `demoehab.yml` | SAS connection string for the `generator-source` Custom Endpoint (Event Hub protocol) |
| `FABRIC_ENDPOINT_CONNECTION_STRING` | `deploy-container-forwarder.yml` | SAS connection string for `voltage-spike-endpoint` (bootstrap server + topic parsed from it for Kafka; passed to the Container App as `SOURCE_CONNECTION_STRING`) |
| `EHTARGET_SEND_CONNECTION_STRING` | `deploy-container-forwarder.yml` | Used only to derive `EH-target`'s namespace FQDN + entity name (not for auth — see above); passed to the Container App as `TARGET_CONNECTION_STRING` |
| `vars.AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_SUBSCRIPTION_ID` | `deploy-container-forwarder.yml`, `update-container-forwarder.yml` | OIDC federated login for the deploying service principal (`my-app-gh-stpa-deploy`) |

All connection strings/keys are only obtainable from the Fabric/Azure portal UI
(no public API for Custom Endpoint keys) and must be rotated via the portal if ever
exposed. **Never paste live connection strings into chat or commit them to the repo.**

### Azure prerequisites

- `EH-target`'s namespace has `disableLocalAuth=true` enforced by policy — SAS-based
  auth will never work for producers on that namespace. Any new consumer/producer
  identity (Container App managed identity, CI service principal, etc.) must be
  granted the **Azure Event Hubs Data Sender** (or Receiver, as applicable) RBAC role
  scoped to the `eh-target` entity, since it cannot use a connection string/SAS key.


