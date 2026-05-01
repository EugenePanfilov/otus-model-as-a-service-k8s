#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"
NAMESPACE="fraud-detection"
DEPLOYMENT_NAME="fraud-api"
SERVICE_NAME="fraud-api-service"
GHCR_SERVER="ghcr.io"
GHCR_USERNAME="${GHCR_USERNAME:-EugenePanfilov}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "ERROR: .env file not found: ${ENV_FILE}"
  echo "Run terraform apply first."
  exit 1
fi

set -a
source "${ENV_FILE}"
set +a

: "${K8S_CLUSTER_ID:?K8S_CLUSTER_ID is required in .env. Run terraform apply first.}"
: "${MLFLOW_TRACKING_URI:?MLFLOW_TRACKING_URI is required in .env}"
: "${S3_ACCESS_KEY:?S3_ACCESS_KEY is required in .env}"
: "${S3_SECRET_KEY:?S3_SECRET_KEY is required in .env}"
: "${S3_ENDPOINT_URL:?S3_ENDPOINT_URL is required in .env}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN is required for GHCR imagePullSecret}"

echo "Bootstrap Kubernetes resources for fraud-api"
echo "Project root: ${PROJECT_ROOT}"
echo "Namespace: ${NAMESPACE}"
echo "K8S cluster id: ${K8S_CLUSTER_ID}"
echo "MLflow URI: ${MLFLOW_TRACKING_URI}"

echo
echo "Getting Kubernetes credentials..."

set +e
timeout 60s yc managed-kubernetes cluster get-credentials \
  --id "${K8S_CLUSTER_ID}" \
  --external \
  --force
YC_GET_CREDENTIALS_EXIT_CODE=$?
set -e

if [[ "${YC_GET_CREDENTIALS_EXIT_CODE}" -ne 0 && "${YC_GET_CREDENTIALS_EXIT_CODE}" -ne 124 ]]; then
  echo "ERROR: failed to get Kubernetes credentials"
  exit "${YC_GET_CREDENTIALS_EXIT_CODE}"
fi

if [[ "${YC_GET_CREDENTIALS_EXIT_CODE}" -eq 124 ]]; then
  echo "WARNING: yc get-credentials timed out, checking whether kubeconfig was updated..."
fi

echo
echo "Checking Kubernetes connection..."
kubectl get nodes --request-timeout=20s

echo
echo "Applying namespace..."
kubectl apply -f "${PROJECT_ROOT}/k8s/namespace.yaml"

echo
echo "Applying ConfigMap..."
kubectl apply -f "${PROJECT_ROOT}/k8s/configmap.yaml"

echo
echo "Creating/updating fraud-api-secret..."
kubectl create secret generic fraud-api-secret \
  -n "${NAMESPACE}" \
  --from-literal=MLFLOW_TRACKING_URI="${MLFLOW_TRACKING_URI}" \
  --from-literal=AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY}" \
  --from-literal=AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY}" \
  --from-literal=MLFLOW_S3_ENDPOINT_URL="${S3_ENDPOINT_URL}" \
  --dry-run=client \
  -o yaml | kubectl apply -f -

echo
echo "Creating/updating GHCR imagePullSecret..."
kubectl create secret docker-registry ghcr-pull-secret \
  -n "${NAMESPACE}" \
  --docker-server="${GHCR_SERVER}" \
  --docker-username="${GHCR_USERNAME}" \
  --docker-password="${GITHUB_TOKEN}" \
  --dry-run=client \
  -o yaml | kubectl apply -f -

echo
echo "Applying Deployment..."
kubectl apply -f "${PROJECT_ROOT}/k8s/deployment.yaml"

echo
echo "Applying Service..."
kubectl apply -f "${PROJECT_ROOT}/k8s/service.yaml"

echo
echo "Waiting for rollout..."
kubectl rollout status deployment/"${DEPLOYMENT_NAME}" -n "${NAMESPACE}"

echo
echo "Kubernetes resources:"
kubectl get all -n "${NAMESPACE}"

echo
echo "Public API endpoint:"
EXTERNAL_IP="$(kubectl get svc "${SERVICE_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.loadBalancer.ingress[0].ip}')"

if [[ -z "${EXTERNAL_IP}" ]]; then
  echo "EXTERNAL-IP is not ready yet. Check later:"
  echo "kubectl get svc -n ${NAMESPACE}"
else
  echo "http://${EXTERNAL_IP}"
  echo
  echo "Smoke checks:"
  curl -s "http://${EXTERNAL_IP}/health" || true
  echo
  curl -s "http://${EXTERNAL_IP}/ready" || true
  echo
fi