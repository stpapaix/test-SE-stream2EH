"""One-off script: check EH-target partition properties to see if any messages have arrived."""
from azure.eventhub import EventHubConsumerClient
from azure.identity import AzureCliCredential, ChainedTokenCredential, DefaultAzureCredential

NAMESPACE = "ehns-test-se-stream2eh-srzfa5vvnklsy.servicebus.windows.net"
EVENTHUB = "EH-target"

credential = ChainedTokenCredential(AzureCliCredential(), DefaultAzureCredential())
client = EventHubConsumerClient(
    fully_qualified_namespace=NAMESPACE,
    eventhub_name=EVENTHUB,
    consumer_group="$Default",
    credential=credential,
)

with client:
    props = client.get_eventhub_properties()
    print(f"Event Hub: {props['eventhub_name']}, partitions: {props['partition_ids']}", flush=True)
    for pid in props["partition_ids"]:
        pp = client.get_partition_properties(pid)
        print(
            f"Partition {pid}: is_empty={pp['is_empty']} "
            f"beginning_seq={pp['beginning_sequence_number']} "
            f"last_seq={pp['last_enqueued_sequence_number']} "
            f"last_enqueued_time={pp['last_enqueued_time_utc']}",
            flush=True,
        )

    print("\n--- Message content (partition 0) ---", flush=True)

    def on_batch(partition_context, event_batch):
        for event in event_batch:
            print(event.body_as_str(), flush=True)
        client.close()

    try:
        client.receive_batch(
            on_event_batch=on_batch,
            max_batch_size=10,
            max_wait_time=5,
            partition_id="0",
            starting_position="-1",
        )
    except Exception as exc:
        print(f"(receive loop stopped: {exc})", flush=True)

