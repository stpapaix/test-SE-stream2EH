#!/usr/bin/env bash
set -euo pipefail

# ---- Parameters ----
SUBSCRIPTION_ID="<subscription-id>"
APP_NAME="<app-registration-name>"
RESOURCE_GROUP="<resource-group-name>"
GITHUB_OWNER="<github-owner>"
GITHUB_REPO="<github-repo>"
BRANCH="main"   # change if deploying from a different branch

# ---- Set active subscription ----
az account set --subscription "$SUBSCRIPTION_ID"

# ---- Create app registration ----
APP_ID=$(az ad app create --display-name "$APP_NAME" --query appId -o tsv)
echo "Created app registration: $APP_ID"

# ---- Create service principal for the app ----
az ad sp create --id "$APP_ID" >/dev/null
echo "Created service principal for app: $APP_ID"

# ---- Assign Contributor role scoped to the resource group ----
az role assignment create \
  --assignee "$APP_ID" \
  --role "Contributor" \
  --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP"
echo "Assigned Contributor role on resource group: $RESOURCE_GROUP"

# ---- Add federated credential trusting GitHub Actions OIDC ----
az ad app federated-credential create --id "$APP_ID" --parameters "{
  \"name\": \"github-$BRANCH-branch\",
  \"issuer\": \"https://token.actions.githubusercontent.com\",
  \"subject\": \"repo:$GITHUB_OWNER/$GITHUB_REPO:ref:refs/heads/$BRANCH\",
  \"audiences\": [\"api://AzureADTokenExchange\"]
}"
echo "Federated credential created for repo:$GITHUB_OWNER/$GITHUB_REPO:ref:refs/heads/$BRANCH"

# ---- Output values needed for GitHub secrets ----
TENANT_ID=$(az account show --query tenantId -o tsv)
echo ""
echo "Add these as GitHub repository secrets:"
echo "AZURE_CLIENT_ID=$APP_ID"
echo "AZURE_TENANT_ID=$TENANT_ID"
echo "AZURE_SUBSCRIPTION_ID=$SUBSCRIPTION_ID"
