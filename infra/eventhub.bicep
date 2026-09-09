// Provisions the Azure Event Hub namespace + entity for the energy telemetry demo.
// EH-target only: EH-source was retired once the generator started publishing solely
// via the Fabric generator-source Custom Endpoint. Deployed in westus to co-locate
// with the Fabric capacity and cut cross-region latency for the spike forwarder.
@description('Azure region for the Event Hub namespace')
param location string = 'westus'

@description('Globally-unique Event Hub namespace name')
param namespaceName string = 'ehns-se-stream2eh-west-${uniqueString(resourceGroup().id)}'

@description('Event Hub entity name for filtered/processed messages sent out from the Eventstream')
param targetEventHubName string = 'EH-target'

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
    disableLocalAuth: true // enforced by org policy regardless; producers/consumers authenticate via Azure AD
  }
}

resource targetEventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: eventHubNamespace
  name: targetEventHubName
  properties: {
    messageRetentionInDays: 1
    partitionCount: 2
  }
}

resource targetEventHubSendRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2024-01-01' = {
  parent: targetEventHub
  name: 'fabric-send-policy'
  properties: {
    rights: [
      'Send'
    ]
  }
}

output namespaceName string = eventHubNamespace.name
output namespaceHostName string = '${eventHubNamespace.name}.servicebus.windows.net'
output targetEventHubName string = targetEventHub.name
