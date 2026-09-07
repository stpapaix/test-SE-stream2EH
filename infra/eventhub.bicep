// Provisions the Azure Event Hub namespace + entity for the energy telemetry demo.
@description('Azure region for the Event Hub namespace')
param location string = resourceGroup().location

@description('Globally-unique Event Hub namespace name')
param namespaceName string = 'ehns-test-se-stream2eh-${uniqueString(resourceGroup().id)}'

@description('Event Hub entity name (the "hub" producers send to / consumers read from)')
param eventHubName string = 'EH-source'

@description('Consumer group name dedicated to the Fabric Eventstream reader')
param fabricConsumerGroupName string = 'fabric-eventstream-cg'

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: namespaceName
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
  properties: {
    isAutoInflateEnabled: false
  }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: eventHubNamespace
  name: eventHubName
  properties: {
    messageRetentionInDays: 1
    partitionCount: 2
  }
}

resource fabricConsumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: eventHub
  name: fabricConsumerGroupName
}

output namespaceName string = eventHubNamespace.name
output namespaceHostName string = '${eventHubNamespace.name}.servicebus.windows.net'
output eventHubName string = eventHub.name
output fabricConsumerGroupName string = fabricConsumerGroup.name
