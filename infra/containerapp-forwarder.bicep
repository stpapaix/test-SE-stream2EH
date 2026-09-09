// Provisions an Azure Container Apps environment running spike-forwarder continuously,
// as an always-on alternative to the GitHub Actions-triggered spike-forwarder workflow.
// Deployed in westus to co-locate with the Fabric capacity and the westus EH-target
// namespace, cutting cross-region latency.
@description('Azure region for all resources')
param location string = 'westus'

@description('Globally-unique Azure Container Registry name (alphanumeric only)')
param acrName string = 'acrsestream2ehwest${uniqueString(resourceGroup().id)}'

@description('Log Analytics workspace name for the Container Apps environment')
param logAnalyticsName string = 'log-se-stream2eh-west'

@description('Container Apps environment name')
param containerAppsEnvName string = 'cae-se-stream2eh-west'

@description('Container App name for the always-on spike forwarder')
param containerAppName string = 'ca-spike-forwarder-west'

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
    adminUserEnabled: true
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
          username: acr.listCredentials().username
          passwordSecretRef: 'acr-password'
        }
      ]
      secrets: [
        {
          name: 'acr-password'
          value: acr.listCredentials().passwords[0].value
        }
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
            cpu: json('0.5')
            memory: '1Gi'
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

output acrLoginServer string = acr.properties.loginServer
output containerAppName string = containerApp.name
output containerAppFqdn string = containerApp.properties.configuration.?ingress.?fqdn ?? 'no ingress (background worker)'
output containerAppPrincipalId string = containerApp.identity.principalId
