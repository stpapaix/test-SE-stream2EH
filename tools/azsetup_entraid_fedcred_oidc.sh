#!/usr/bin/env bash
set -euo pipefail

# ---- Parameters ----
SUBSCRIPTION_ID="ac94ffc0-ccd7-43e8-bf7b-288760e0f960"
APP_NAME="my-app-gh-stpa-deploy"
RESOURCE_GROUP="test-SE-stream2EH"
GITHUB_OWNER="stpapaix"
GITHUB_REPO="test-SE-stream2EH"
BRANCH="main"   # change if deploying from a different branch

# ---- Set active subscription ----
az account set --subscription "$SUBSCRIPTION_ID"

# ---- Clean slate: remove any previous app registration with the same name ----
EXISTING_APP_ID=$(az ad app list --display-name "$APP_NAME" --query "[0].appId" -o tsv)
if [ -n "$EXISTING_APP_ID" ]; then
  echo "Deleting existing app registration: $EXISTING_APP_ID"
  az ad app delete --id "$EXISTING_APP_ID"
fi

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

# ---- Resolve GitHub's immutable owner/repo IDs (repos opted into immutable OIDC subject claims embed these) ----
GH_OWNER_ID=$(curl -s "https://api.github.com/users/$GITHUB_OWNER" | grep -m1 '"id"' | grep -oE '[0-9]+')
GH_REPO_ID=$(curl -s "https://api.github.com/repos/$GITHUB_OWNER/$GITHUB_REPO" | grep -m1 '"id"' | grep -oE '[0-9]+')
SUBJECT="repo:$GITHUB_OWNER@$GH_OWNER_ID/$GITHUB_REPO@$GH_REPO_ID:ref:refs/heads/$BRANCH"
echo "Resolved OIDC subject: $SUBJECT"

# ---- Add federated credential trusting GitHub Actions OIDC (JSON written to file to avoid shell-quoting issues) ----
cat > fedcred.json <<EOF
{
  "name": "github-$BRANCH-branch",
  "issuer": "https://token.actions.githubusercontent.com",
  "subject": "$SUBJECT",
  "audiences": ["api://AzureADTokenExchange"]
}
EOF
az ad app federated-credential create --id "$APP_ID" --parameters "@fedcred.json"
rm -f fedcred.json
echo "Federated credential created for subject: $SUBJECT"

# ---- Output values needed for GitHub secrets ----
TENANT_ID=$(az account show --query tenantId -o tsv)
echo ""
echo "Add these as GitHub repository variables:"
echo "AZURE_CLIENT_ID=$APP_ID"
echo "AZURE_TENANT_ID=$TENANT_ID"
echo "AZURE_SUBSCRIPTION_ID=$SUBSCRIPTION_ID"
