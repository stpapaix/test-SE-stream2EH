// Provisions an Azure Container Apps environment running spike-forwarder continuously,
// as an always-on alternative to the GitHub Actions-triggered spike-forwarder workflow.
// Both can run side by side for latency comparison.
@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Globally-unique Azure Container Registry name (alphanumeric only)')
param acrName string = 'acrsestream2eh${uniqueString(resourceGroup().id)}'

@description('Log Analytics workspace name for the Container Apps environment')
param logAnalyticsName string = 'log-se-stream2eh'

@description('Container Apps environment name')
param containerAppsEnvName string = 'cae-se-stream2eh'

@description('Container App name for the always-on spike forwarder')
param containerAppName string = 'ca-spike-forwarder'

@description('Container image to run (updated after az acr build pushes a new tag)')
param containerImage string = 'mcr.microsoft.com/k8se/quickstart:latest'

@description('SAS connection string for the Fabric voltage-spike-endpoint custom endpoint (Kafka)')
@secure()
param sourceConnectionString string

@description('SAS connection string for EH-target (fabric-send-policy, Send rights)')
@secure()
param targetConnectionString string

resource acr 'Microsoft.ContainerRegistry/registries@2023-11-01-preview' = {
  name: acrName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
  }
}

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

resource containerAppsEnv 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: containerAppsEnvName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
  }
}

resource containerApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: containerAppName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    managedEnvironmentId: containerAppsEnv.id
    configuration: {
      activeRevisionsMode: 'Single'
      registries: [
        {
          server: acr.properties.loginServer
          identity: 'system'
        }
      ]
      secrets: [
        {
          name: 'source-connection-string'
          value: sourceConnectionString
        }
        {
          name: 'target-connection-string'
          value: targetConnectionString
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'spike-forwarder'
          image: containerImage
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
          env: [
            {
              name: 'SOURCE_CONNECTION_STRING'
              secretRef: 'source-connection-string'
            }
            {
              name: 'TARGET_CONNECTION_STRING'
              secretRef: 'target-connection-string'
            }
            {
              name: 'SOURCE_CONSUMER_GROUP'
              value: '$Default'
            }
            {
              name: 'KAFKA_AUTO_OFFSET_RESET'
              value: 'earliest'
            }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
}

// Grant the Container App's managed identity permission to pull from ACR.
resource acrPullRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acr.id, containerApp.id, 'AcrPull')
  scope: acr
  properties: {
    principalId: containerApp.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d') // AcrPull
  }
}

output acrLoginServer string = acr.properties.loginServer
output containerAppName string = containerApp.name
output containerAppFqdn string = containerApp.properties.configuration.?ingress.?fqdn ?? 'no ingress (background worker)'
